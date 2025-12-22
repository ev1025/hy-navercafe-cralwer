import os
import asyncio
import aiohttp
import gspread
import pandas as pd
import warnings
import time
import random
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv 

load_dotenv()
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

# ==========================================
# 1. 설정
# ==========================================
INITIAL_FULL_SCAN = True 
FORCE_COLLECT = True  # 중복 무시 수집

KST = timezone(timedelta(hours=9))

def get_timestamp(year, month, day, hour=0, minute=0, second=0):
    dt = datetime(year, month, day, hour, minute, second, tzinfo=KST)
    return dt.timestamp()

now_kst = datetime.now(timezone.utc).astimezone(KST)
today_midnight_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)

if INITIAL_FULL_SCAN:
    # 21일 00:00:00 ~ 23:59:59 KST
    START_TS = get_timestamp(2025, 12, 21, 0, 0, 0)
    END_TS = get_timestamp(2025, 12, 21, 23, 59, 59)
else:
    yesterday = today_midnight_kst - timedelta(days=1)
    START_TS = yesterday.timestamp()
    END_TS = (today_midnight_kst - timedelta(seconds=1)).timestamp()

print(f"==================================================")
print(f"[설정 확인] 수집 범위 (KST): {datetime.fromtimestamp(START_TS, KST)} ~ {datetime.fromtimestamp(END_TS, KST)}")
print(f"==================================================\n")

cafes_to_scrape = {"토마스": 17175596, "수만휘": 10197921, "로물콘": 28699715}
boards_to_scrape = {17175596: [0], 10197921: [0], 28699715: [0]}

GCP_SERVICE_KEY = "naver_cralwer_service_key.json"
googlesheet_url = "https://docs.google.com/spreadsheets/d/1vXco0waE_iBVhmXUqMe7O56KKSjY6bn4MiC3btoAPS8/edit?gid=0#gid=0"

# ==========================================
# 2. 구글 시트 연결
# ==========================================
def get_raw_sheet():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GCP_SERVICE_KEY, scope)
        client = gspread.authorize(creds)
        return client.open_by_url(googlesheet_url).worksheet("원본데이터")
    except Exception as e:
        print(f"[Error] 구글 시트 연결 실패: {e}"); return None

raw_sheet = get_raw_sheet()
existing_posts = set()
if raw_sheet and not FORCE_COLLECT:
    try:
        all_data = raw_sheet.get_all_records()
        for row in all_data:
            existing_posts.add((str(row.get('사이트','')).strip(), str(row.get('게시글번호','')).strip()))
    except: pass

# ==========================================
# 3. 데이터 수집 함수
# ==========================================
async def fetch_article_detail(session, cafe_name, cafe_id, aid):
    url = f"https://article.cafe.naver.com/gw/v3/cafes/{cafe_id}/articles/{aid}?useCafeId=true&requestFrom=A"
    try:
        # 타임아웃을 넉넉히 주어 연결 끊김 방지
        async with session.get(url, timeout=20) as resp:
            if resp.status == 429: # 너무 많은 요청
                return "RETRY"
            if resp.status != 200: return None
            
            data = await resp.json(content_type=None)
            res = data.get('result', {})
            art = res.get('article', {})
            if not art: return None
            
            html = art.get('contentHtml') or res.get('scrap', {}).get('contentHtml', '')
            content = BeautifulSoup(html, 'html.parser').get_text(strip=True, separator='\n')
            comments = [BeautifulSoup(c.get('content', ''), 'html.parser').get_text(strip=True, separator='\n') 
                        for c in res.get('comments', {}).get('items', []) if c.get('content')]
            
            write_ts = art.get('writeDate', 0) 
            post_date = datetime.fromtimestamp(write_ts/1000, KST).strftime("%Y-%m-%d %H:%M:%S")

            return {
                '사이트': cafe_name, '날짜': post_date, '제목': art.get('subject', '제목 없음'),
                '본문': content, '댓글': "\n".join([f"[댓글{i+1}]\n{t}\n" for i, t in enumerate(comments)]),
                '게시글번호': int(aid)
            }
    except: return None

async def fetch_board_page(session, cafe_id, menu_id, page):
    url = f"https://apis.naver.com/cafe-web/cafe-boardlist-api/v1/cafes/{cafe_id}/menus/{menu_id}/articles?page={page}&sortBy=TIME"
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('result', {}).get('articleList', [])
    except: pass
    return []

async def scan_board(session, cafe_name, cafe_id, menu_id, start_ts, end_ts):
    article_ids = []
    BATCH_SIZE = 5 
    for start_page in range(1, 3001, BATCH_SIZE): 
        tasks = [fetch_board_page(session, cafe_id, menu_id, p) for p in range(start_page, start_page + BATCH_SIZE)]
        results = await asyncio.gather(*tasks)
        
        found_any_in_batch = False
        batch_oldest_ts = None

        for articles in results:
            if not articles: continue
            for item in articles:
                info = item.get('item', {})
                item_ts = info.get('writeDateTimestamp') / 1000
                batch_oldest_ts = item_ts
                aid = str(info.get('articleId')).strip()

                if start_ts <= item_ts <= end_ts:
                    if FORCE_COLLECT or (str(cafe_name).strip(), aid) not in existing_posts:
                        article_ids.append(aid)
                
                if item_ts >= start_ts:
                    found_any_in_batch = True

        if batch_oldest_ts and batch_oldest_ts < start_ts:
            print(f"  [Info] {cafe_name}: {datetime.fromtimestamp(batch_oldest_ts, KST)} 확인. 탐색 종료.")
            break
        
        if start_page % 50 == 1:
            print(f"  [Progress] {cafe_name} {start_page}P 스캔 중... (현재: {datetime.fromtimestamp(batch_oldest_ts, KST) if batch_oldest_ts else 'N/A'})")
            
    return list(dict.fromkeys(article_ids))

async def main():
    my_cookie = os.getenv("NAVER_COOKIE_STRING")
    if not my_cookie: print("쿠키 없음"); return
    
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "cookie": my_cookie,
        "x-cafe-product": "pc",
        "referer": "https://cafe.naver.com/"
    }
    
    final_data = []
    async with aiohttp.ClientSession(headers=headers) as session:
        for cafe_name, cafe_id in cafes_to_scrape.items():
            print(f"\n[Step 1] '{cafe_name}' ID 스캔 시작...")
            for bid in boards_to_scrape.get(cafe_id, [0]):
                aids = await scan_board(session, cafe_name, cafe_id, bid, START_TS, END_TS)
                
                if aids:
                    print(f"[Step 2] '{cafe_name}' 본문 수집 시작 ({len(aids)}건)...")
                    
                    # [★해결책] 6,000건을 한꺼번에 하지 않고 20개씩 끊어서 수집
                    CHUNK_SIZE = 20 
                    for i in range(0, len(aids), CHUNK_SIZE):
                        chunk = aids[i : i + CHUNK_SIZE]
                        tasks = [fetch_article_detail(session, cafe_name, cafe_id, aid) for aid in chunk]
                        results = await asyncio.gather(*tasks)
                        
                        valid_results = [r for r in results if r and r != "RETRY"]
                        final_data.extend(valid_results)
                        
                        # 진행률 표시 및 서버 부하 방지용 짧은 휴식
                        if (i // CHUNK_SIZE) % 5 == 0:
                            print(f"    ... 수집 진행 중: {i}/{len(aids)} 완료 (현재까지 총 {len(final_data)}건 확보)")
                        
                        await asyncio.sleep(random.uniform(0.3, 0.7))

    if final_data and raw_sheet:
        df = pd.DataFrame(final_data).sort_values(by=['날짜', '게시글번호'])
        data_to_upload = df.values.tolist()
        
        print(f"\n[Step 3] 구글 시트 업로드 중 ({len(data_to_upload)}건)...")
        # 구글 시트도 한꺼번에 너무 많이 올리면 에러날 수 있으므로 1000개씩 분할 업로드
        for i in range(0, len(data_to_upload), 1000):
            raw_sheet.append_rows(data_to_upload[i:i+1000], value_input_option='USER_ENTERED')
            
        print(f"[Success] 모든 수집 및 업로드 완료!")
    else:
        print("\n[Info] 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())