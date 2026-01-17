import os, json, random ,asyncio, re
from tqdm import tqdm
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
import openai
from dotenv import load_dotenv
# conda activate recent
# cd /c/Users/ENVY/Desktop/youtube/hy-navercafe-cralwer
load_dotenv()

# ==========================================
# 1. 환경 변수 및 설정
# ==========================================
YOUTUBE_API_KEY = os.environ.get("GCP_API_KEY") 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
GCP_SA_KEY_STR = os.environ.get("GCP_SA_KEY") 
PROXY_USERNAME = os.environ.get("PROXY_USERNAME")
PROXY_PASSWORD = os.environ.get("PROXY_PASSWORD")

TARGET_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vXco0waE_iBVhmXUqMe7O56KKSjY6bn4MiC3btoAPS8/edit"
SOURCE_SHEET_NAME = "유튜브정리"
TARGET_SHEET_NAME = "유튜브 요약"

# dynamic_start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
# START_DATE = dynamic_start_date
START_DATE = "2024-01-01" 
TEST_NUM = None # None으로 하면 전체 수집

SHEET_CELL_LIMIT = 45000 
GPT_INPUT_LIMIT = 100000 
CONCURRENT_LIMIT = 15 
semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

aclient = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)

# ==========================================
# [공통] 재시도 로직
# ==========================================
async def retry_action(func, *args, retries=3, delay=60, description="작업"):
    for attempt in range(retries):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args)
            else:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, func, *args)
        except Exception as e:
            if attempt == retries - 1:
                return None
            await asyncio.sleep(delay)
    return None

# ==========================================
# 2. 구글 시트 & 링크 추출
# ==========================================
def connect_google_sheet(sheet_name=None):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if GCP_SA_KEY_STR:
        creds = Credentials.from_service_account_info(json.loads(GCP_SA_KEY_STR), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(TARGET_SPREADSHEET_URL)
    
    if sheet_name:
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            if sheet_name == TARGET_SHEET_NAME:
                sheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows=100, cols=20)
                sheet.append_row(["채널명", "날짜", "제목", "스크립트", "GPT요약", "URL"])
            else:
                raise Exception(f"❌ '{sheet_name}' 시트를 찾을 수 없습니다.")
        return sheet
    return spreadsheet

def extract_links_using_api(spreadsheet_url, sheet_name):
    try:
        spreadsheet_id = spreadsheet_url.split("/d/")[1].split("/")[0]
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        if GCP_SA_KEY_STR:
            creds = Credentials.from_service_account_info(json.loads(GCP_SA_KEY_STR), scopes=scopes)
        else:
            creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
        service = build('sheets', 'v4', credentials=creds)

        range_name = f"{sheet_name}!C2:C"
        print(f"📡 구글 시트 API 요청 중... ({range_name})")
        fields_param = "sheets(data(rowData(values(hyperlink,userEnteredValue,formattedValue))))"
        result = service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=[range_name], fields=fields_param).execute()

        extracted_data = []
        if 'sheets' in result and 'data' in result['sheets'][0]:
            rows = result['sheets'][0]['data'][0].get('rowData', [])
            for row in rows:
                if not row.get('values'):
                    extracted_data.append(None); continue
                cell_data = row['values'][0]
                url = None
                if 'hyperlink' in cell_data: url = cell_data['hyperlink']
                elif 'userEnteredValue' in cell_data and 'formulaValue' in cell_data['userEnteredValue']:
                    match = re.search(r'"(https?://.*?)"', cell_data['userEnteredValue']['formulaValue'])
                    if match: url = match.group(1)
                elif 'formattedValue' in cell_data and str(cell_data['formattedValue']).startswith("http"):
                    url = cell_data['formattedValue']
                extracted_data.append(url)
        return extracted_data
    except Exception as e:
        print(f"❌ 시트 API 에러: {e}"); return []

def get_channel_id_from_url(youtube, url):
    if not url or "youtube.com" not in url: return None
    try:
        if "@" in url:
            handle = url.split("@")[-1].split("/")[0].split("?")[0]
            res = youtube.channels().list(part="id", forHandle=f"@{handle}").execute()
            if res.get("items"): return res["items"][0]["id"]
        if "/channel/" in url:
            return url.split("/channel/")[-1].split("/")[0].split("?")[0]
    except: return None
    return None

def fetch_channel_ids_from_sheet():
    urls = extract_links_using_api(TARGET_SPREADSHEET_URL, SOURCE_SHEET_NAME)
    if not urls: return []
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    channel_ids = []
    for url in urls:
        if not url: continue
        ch_id = get_channel_id_from_url(youtube, url)
        if ch_id: channel_ids.append(ch_id)
    unique_ids = list(set(channel_ids))
    print(f"✅ 최종 식별된 채널 ID: {len(unique_ids)}개")
    return unique_ids

# ==========================================
# 4. 영상 목록 수집 (쇼츠 제외)
# ==========================================
def get_all_videos(channel_id, start_date):
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        res = youtube.channels().list(id=channel_id, part="snippet,contentDetails").execute()
        if not res["items"]: return [], "Unknown"
        channel_title = res["items"][0]["snippet"]["title"]
        
        # [UULF 적용] 쇼츠 제외
        real_channel_id = res["items"][0]["id"]
        playlist_id = real_channel_id.replace("UC", "UULF", 1)
        
        videos = []
        next_page_token = None
        stop_collecting = False
        while not stop_collecting:
            try:
                pl_res = youtube.playlistItems().list(playlistId=playlist_id, part="snippet", maxResults=50, pageToken=next_page_token).execute()
                for item in pl_res["items"]:
                    video_id = item["snippet"]["resourceId"]["videoId"]
                    title = item["snippet"]["title"]
                    published_at = item["snippet"]["publishedAt"].split("T")[0]
                    if published_at < start_date:
                        stop_collecting = True; break
                    videos.append({"id": video_id, "title": title, "date": published_at})
                next_page_token = pl_res.get("nextPageToken")
                if not next_page_token: break
            except: break
        return videos, channel_title
    except: return [], "Unknown"

# ==========================================
# 5~7. 처리 로직 (자막, 요약, 워커)
# ==========================================
def get_transcript_sync(video_id):
    if not PROXY_USERNAME or not PROXY_PASSWORD: raise ValueError("프록시 정보 없음")
    proxy_config = WebshareProxyConfig(proxy_username=PROXY_USERNAME, proxy_password=PROXY_PASSWORD)
    ytt_api = YouTubeTranscriptApi(proxy_config=proxy_config)
    transcript_data = ytt_api.fetch(video_id, languages=['ko'])
    return " ".join(snippet.text for snippet in transcript_data.snippets)

async def summarize_text_task(text):
    if not text: return "자막 없음"
    input_text = text[:GPT_INPUT_LIMIT]
    system_prompt = "유튜브 영상을 분석하여 핵심 내용 5~10가지를 한국어 불렛 포인트로(-)요약하세요."
    response = await aclient.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": input_text}]
    )
    return response.choices[0].message.content

async def process_video(video, channel_name, pbar, processed_in_channel, channels_task_counts):
    async with semaphore: 
        video_url = f"https://www.youtube.com/watch?v={video['id']}"
        await asyncio.sleep(random.uniform(0.5, 1.5)) 
        
        script = await retry_action(get_transcript_sync, video['id'], retries=3, delay=60, description=f"[{channel_name}] 자막")
        summary = "요약 불가"
        saved_script = "자막 없음"
        
        if script:
            summary_result = await retry_action(summarize_text_task, script, retries=3, delay=60, description=f"[{channel_name}] 요약")
            if summary_result: summary = summary_result
            saved_script = script[:SHEET_CELL_LIMIT] + "...(절삭)" if len(script) > SHEET_CELL_LIMIT else script
        
        pbar.update(1)
        processed_in_channel[channel_name] = processed_in_channel.get(channel_name, 0) + 1
        
        # 섞여서 완료되므로 완료 메시지가 산발적으로 뜰 수 있음
        if processed_in_channel[channel_name] == channels_task_counts[channel_name]:
            pbar.write(f"✅ {channel_name} 완료 ({channels_task_counts[channel_name]}개)")
    
        return [channel_name, video['date'], video['title'], saved_script, summary, video_url]

# # ==========================================
# # [NEW] 9. 실패 항목 재시도 (A/S) 기능
# # ==========================================
# async def repair_failed_rows(sheet):
#     print("\n🔧 [A/S 단계] '요약 불가' 항목 재작업 시작...")
    
#     # 1. 시트 데이터 전체 읽기
#     try:
#         # get_all_values는 동기 함수이므로 retry_action으로 보호하지 않아도 되지만, 
#         # API 오류 가능성이 있으므로 간단한 try-except 처리
#         rows = sheet.get_all_values()
#     except Exception as e:
#         print(f"❌ 시트 읽기 실패: {e}")
#         return

#     # 2. 실패한 행 추출 (헤더 제외)
#     failed_tasks = []
#     # rows[i]는 엑셀의 i+1행 (0번은 헤더)
#     for i, row in enumerate(rows):
#         if i == 0: continue 
        
#         # 안전장치: 행 데이터가 부족한 경우 건너뜀
#         if len(row) < 6: continue
        
#         # row 인덱스: 0:채널, 1:날짜, 2:제목, 3:스크립트, 4:요약, 5:URL
#         script = row[3]
#         summary = row[4]
#         url = row[5]
        
#         # 조건: '요약 불가'이거나 비어있는데, URL은 정상적인 경우
#         if (summary.strip() == "요약 불가" or summary.strip() == "") and url.strip().startswith("http"):
#             failed_tasks.append({
#                 "row_idx": i + 1, # 엑셀 행 번호 (1부터 시작)
#                 "channel": row[0],
#                 "script": script,
#                 "url": url
#             })
            
#     if not failed_tasks:
#         print("✨ 모든 항목이 정상입니다. 재작업할 것이 없습니다.")
#         return

#     print(f"⚠️ 총 {len(failed_tasks)}개의 실패 항목 발견! 심폐소생술 시도합니다...")

#     # 3. 재작업 워커 정의
#     async def repair_worker(task):
#         async with semaphore: # 동시 실행 제한
#             row_num = task['row_idx']
#             url = task['url']
#             channel_name = task['channel']
#             current_script = task['script']
            
#             # Video ID 추출
#             try:
#                 if "v=" in url:
#                     video_id = url.split("v=")[1].split("&")[0]
#                 else:
#                     return None
#             except:
#                 return None

#             # [단계 1] 자막이 없다면 자막부터 다시 시도
#             if not current_script or current_script == "자막 없음":
#                 await asyncio.sleep(random.uniform(0.5, 1.5))
#                 # 재시도 횟수 2회
#                 fetched_script = await retry_action(get_transcript_sync, video_id, retries=2, delay=30, description=f"[{channel_name}] 자막 재수집")
#                 if fetched_script:
#                     current_script = fetched_script
            
#             # [단계 2] 자막이 확보되었다면 요약 재시도
#             new_summary = "요약 불가"
#             final_script = current_script
            
#             if current_script and current_script != "자막 없음":
#                 # 요약 재시도 (재시도 횟수 2회)
#                 summary_res = await retry_action(summarize_text_task, current_script, retries=2, delay=30, description=f"[{channel_name}] 요약 재시도")
#                 if summary_res:
#                     new_summary = summary_res
            
#             # [단계 3] 결과가 개선되었으면 리턴 ('요약 불가' 탈출했거나, 자막이라도 건졌거나)
#             if new_summary != "요약 불가" or (current_script != "자막 없음" and task['script'] == "자막 없음"):
#                 # 스크립트 길이 절삭
#                 if len(current_script) > SHEET_CELL_LIMIT:
#                     final_script = current_script[:SHEET_CELL_LIMIT] + "...(절삭)"
                
#                 return (row_num, final_script, new_summary, channel_name)
            
#             return None

#     # 4. 재작업 실행
#     pbar = tqdm(total=len(failed_tasks), desc="🔧 A/S 진행 중")
#     tasks = [asyncio.create_task(repair_worker(t)) for t in failed_tasks]
    
#     success_count = 0
    
#     for future in asyncio.as_completed(tasks):
#         result = await future
#         pbar.update(1)
        
#         if result:
#             row_num, script_txt, summary_txt, ch_name = result
            
#             # 구글 시트 특정 셀 업데이트 (D열=자막, E열=요약)
#             # update는 API 호출이므로 retry 적용
#             cell_range = f"D{row_num}:E{row_num}"
#             try:
#                 await retry_action(
#                     sheet.update, cell_range, [[script_txt, summary_txt]],
#                     retries=3, delay=60, description=f"{ch_name} 행 업데이트"
#                 )
#                 success_count += 1
#                 pbar.write(f"✅ {ch_name} (행 {row_num}) 복구 성공!")
#             except Exception as e:
#                 pbar.write(f"❌ 행 {row_num} 업데이트 실패: {e}")

#     pbar.close()
#     print(f"✨ A/S 완료: 총 {success_count}개 항목을 살려냈습니다!")

# ==========================================
# 8. 메인 실행 (수정됨)
# ==========================================
async def async_main():
    target_channel_ids = fetch_channel_ids_from_sheet()
    if not target_channel_ids: return
    
    sheet = connect_google_sheet(TARGET_SHEET_NAME)
    try: existing_urls = set(sheet.col_values(6))
    except: existing_urls = set()

    all_video_tasks_info = []
    channels_task_counts = {}
    channel_names_display = []

    print(f"\n📅 기준 날짜: {START_DATE}")
    print(f"🧪 수집 모드: 채널당 {TEST_NUM if TEST_NUM else '전체'}")
    print("-" * 50)

    # 1. 태스크 목록 생성
    for ch_id in target_channel_ids:
        videos, channel_name = get_all_videos(ch_id, START_DATE)
        channel_names_display.append(channel_name)
        
        new_videos = []
        for v in videos:
            if f"https://www.youtube.com/watch?v={v['id']}" not in existing_urls:
                new_videos.append(v)
        
        if TEST_NUM and len(new_videos) > TEST_NUM:
            new_videos = new_videos[:TEST_NUM]
        
        all_video_tasks_info.extend([(v, channel_name) for v in new_videos])
        channels_task_counts[channel_name] = len(new_videos)
        
        if len(new_videos) > 0:
            print(f"   👉 {channel_name}: {len(new_videos)}개")

    total_count = len(all_video_tasks_info)
    
    # [수정] 수집할 게 없어도, 바로 종료하지 않고 '재작업(A/S)' 단계로 넘어가게 함
    if total_count > 0:
        print("-" * 50)
        print(f"🔢 총 {total_count}개 영상 -> 무작위 섞어서 동시 처리 시작")
        print("-" * 50)

        random.shuffle(all_video_tasks_info)

        processed_in_channel = {}
        buffer = []
        
        with tqdm(total=total_count, desc="⚡ 고속 처리 중") as pbar:
            tasks = [
                asyncio.create_task(process_video(v, name, pbar, processed_in_channel, channels_task_counts)) 
                for v, name in all_video_tasks_info
            ]
            
            for future in asyncio.as_completed(tasks):
                result = await future 
                if result: buffer.append(result)
                
                if len(buffer) >= 50:
                    pbar.write(f"🚀 버퍼 가득 참 (50개) -> 구글 시트 즉시 저장")
                    upload_data = list(buffer)
                    buffer.clear() 
                    await retry_action(sheet.append_rows, upload_data, retries=5, delay=60, description="구글 시트 저장")
                    
            if buffer:
                pbar.write(f"🚀 나머지 {len(buffer)}개 -> 구글 시트 저장")
                await retry_action(sheet.append_rows, buffer, retries=5, delay=60, description="마지막 저장")
    else:
        print("🎉 새로 수집할 영상이 없습니다. 바로 A/S 단계로 넘어갑니다.")

    # # ==========================================
    # # [마지막 단계] 실패한 항목 재시도 실행
    # # ==========================================
    # print("-" * 50)
    # await repair_failed_rows(sheet)
    # print("-" * 50)

    print("\n🎉 모든 작업(수집+복구)이 완료되었습니다!")

if __name__ == "__main__":
    asyncio.run(async_main())