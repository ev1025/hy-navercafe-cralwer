import os
import asyncio
import aiohttp
import gspread
import pandas as pd
import warnings
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv 
load_dotenv()
# 경고 무시
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
INITIAL_FULL_SCAN = False 

today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

if INITIAL_FULL_SCAN:
    start_date_target = datetime(2025, 12, 18)
    end_date_target = today - timedelta(seconds=1)
else:
    # [데일리 모드] 어제 00:00:00 ~ 어제 23:59:59
    start_date_target = today - timedelta(days=1)
    end_date_target = today - timedelta(seconds=1)

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
        print(f"[Error] 구글 시트 연결 실패: {e}")
        return None

print("\n[Init] 구글 시트 연결 및 중복 데이터 로딩 중...")
raw_sheet = get_raw_sheet()

limit_dup = (datetime.now() - timedelta(days=5)).date()
existing_posts = set()

if raw_sheet:
    try:
        all_data = raw_sheet.get_all_records()
        for row in all_data:
            try:
                raw_date = str(row['날짜'])
                row_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
                if row_date >= limit_dup:
                    existing_posts.add((row['사이트'], str(row['게시글번호'])))
            except (ValueError, KeyError, IndexError):
                continue
        print(f"[Init] 기존 데이터 {len(existing_posts)}건 로드 완료 (기준: {limit_dup} 이후).")
    except Exception as e:
        print(f"[Init] 기존 데이터 로드 중 오류 (첫 실행이면 무시): {e}")

# ==========================================
# 3. 데이터 수집 함수
# ==========================================
async def fetch_article_detail(session, cafe_name, cafe_id, menu_id, aid):
    url = f"https://article.cafe.naver.com/gw/v3/cafes/{cafe_id}/articles/{aid}?useCafeId=true&requestFrom=A"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200: return None
            data = await resp.json(content_type=None)
            res = data.get('result', {})
            art = res.get('article', {})
            
            html = art.get('contentHtml') or res.get('scrap', {}).get('contentHtml', '')
            parser = 'lxml' if 'lxml' in globals() else 'html.parser'
            content = BeautifulSoup(html, parser).get_text(strip=True, separator='\n') if html else ""
            
            raw_comments = [BeautifulSoup(c.get('content', ''), parser).get_text(strip=True, separator='\n') 
                            for c in res.get('comments', {}).get('items', []) if c.get('content')]
            
            formatted_comments = []
            for i, text in enumerate(raw_comments, 1):
                formatted_comments.append(f"[댓글{i}]\n {text}\n")
            
            write_ts = art.get('writeDate', 0)
            post_datetime = datetime.fromtimestamp(write_ts/1000).strftime("%Y-%m-%d %H:%M:%S")

            return {
                '사이트': cafe_name,
                '날짜': post_datetime,
                '제목': art.get('subject', '제목 없음'),
                '본문': content,
                '댓글': "\n".join(formatted_comments),
                '게시글번호': int(aid)
            }
    except: return None

async def fetch_board_page(session, cafe_id, menu_id, page):
    url = f"https://apis.naver.com/cafe-web/cafe-boardlist-api/v1/cafes/{cafe_id}/menus/{menu_id}/articles?page={page}&sortBy=TIME"
    try:
        async with session.get(url, timeout=5) as resp:
            if resp.status != 200: return []
            data = await resp.json()
            return data.get('result', {}).get('articleList', [])
    except:
        return []

async def scan_board(session, cafe_name, cafe_id, menu_id, start_dt, end_dt):
    article_ids = []
    BATCH_SIZE = 5 
    
    for start_page in range(1, 1001, BATCH_SIZE):
        pages_to_fetch = range(start_page, start_page + BATCH_SIZE)
        tasks = [fetch_board_page(session, cafe_id, menu_id, p) for p in pages_to_fetch]
        results = await asyncio.gather(*tasks)
        
        should_stop_board = False
        found_count_in_batch = 0 
        
        for articles in results:
            if not articles: continue
            found_count_in_batch += 1
            
            for item in articles:
                info = item.get('item', {})
                ts = info.get('writeDateTimestamp')
                if not ts: continue
                
                post_dt = datetime.fromtimestamp(ts/1000)
                aid = str(info.get('articleId'))

                if post_dt < start_dt:
                    should_stop_board = True
                    break
                
                if post_dt <= end_dt:
                    if (cafe_name, aid) not in existing_posts:
                        article_ids.append(aid)
                    elif not INITIAL_FULL_SCAN: 
                        should_stop_board = True
                        break
            
            if should_stop_board: break
        
        if found_count_in_batch == 0: break
        if should_stop_board: break
            
    return article_ids

# ==========================================
# 4. 메인 실행 루프
# ==========================================
async def main():
    print(f"\n[Start] 수집 기간: {start_date_target} ~ {end_date_target}")
    my_cookie = os.getenv("NAVER_COOKIE_STRING")    
    if not my_cookie:
        print("[Critical] 쿠키가 없습니다. Github Secrets(NAVER_COOKIE_STRING) 설정을 확인하세요.")
        return

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "cookie": my_cookie,
        "accept": "application/json, text/plain, */*",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "origin": "https://cafe.naver.com",
        "referer": "https://cafe.naver.com/",
        "x-cafe-product": "pc"
    }
    
    final_data = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for cafe_name, cafe_id in cafes_to_scrape.items():
            print(f"\n[Debug] '{cafe_name}' 접속 시도...")
            
            # 메뉴 정보 가져오기 (쿠키 유효성 체크 겸용)
            all_m = []
            menu_url = f"https://apis.naver.com/cafe-web/cafe-cafemain-api/v1.0/cafes/{cafe_id}/menus"
            
            async with session.get(menu_url) as resp:
                # 쿠키 만료 체크
                if "nidlogin.login" in str(resp.url) or resp.status in [401, 403]:
                     print(f"  [Error] 쿠키가 만료되었습니다. 로컬에서 cookie.py를 실행해 Secrets를 갱신해주세요.")
                     return

                if resp.status == 200:
                    try:
                        m_data = await resp.json()
                        res = m_data.get('result', {})
                        all_m = res.get('menus', []) + res.get('linkMenus', [])
                    except: pass
            
            target_ids = boards_to_scrape.get(cafe_id, [])
            
            if not target_ids:
                print("  [Info] 전체 게시판 수집 모드")
                board_ids = [m['menuId'] for m in all_m if m.get('menuId')]
            else:
                board_ids = target_ids 

            for bid in board_ids:
                session.headers.update({"Referer": f"https://cafe.naver.com/ArticleList.nhn?search.clubid={cafe_id}&search.menuid={bid}"})
                
                aids = await scan_board(session, cafe_name, cafe_id, bid, start_date_target, end_date_target)
                
                if aids:
                    print(f"  -> {len(aids)}개 게시글 상세 수집 시작...")
                    CHUNK_SIZE = 30
                    for i in range(0, len(aids), CHUNK_SIZE):
                        chunk = aids[i : i + CHUNK_SIZE]
                        tasks = [fetch_article_detail(session, cafe_name, cafe_id, bid, aid) for aid in chunk]
                        details = await asyncio.gather(*tasks)
                        valid_details = [d for d in details if d]
                        final_data.extend(valid_details)
                        await asyncio.sleep(0.5)

    if final_data and raw_sheet:
        df = pd.DataFrame(final_data)
        df = df.sort_values(by=['날짜', '게시글번호'], ascending=[True, True])
        
        desired_order = ['사이트', '날짜', '제목', '본문', '댓글', '게시글번호']
        final_columns = [col for col in desired_order if col in df.columns]
        df = df[final_columns]
        
        try:
            first_row = raw_sheet.row_values(1)
            if first_row != final_columns:
                raw_sheet.insert_row(final_columns, 1)
        except:
            raw_sheet.insert_row(final_columns, 1)
            
        print(f"\n[Save] {len(df)}건 업로드 중...")
        raw_sheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
        print(f"[Success] 완료.")
    else:
        print("\n[Info] 신규 데이터 없음.")

if __name__ == "__main__":
    try:
        import nest_asyncio
        nest_asyncio.apply()
    except ImportError:
        pass
    asyncio.run(main())