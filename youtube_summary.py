import os
import json
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
import openai

# ==========================================
# 1. 환경 변수 및 설정
# ==========================================
YOUTUBE_API_KEY = os.environ.get("GCP_API_KEY") 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
GCP_SA_KEY_STR = os.environ.get("GCP_SA_KEY") 
CHANNEL_IDS_STR = os.environ.get("CHANNEL_ID") 

# [수정됨] 파일 이름 검색 대신 URL과 시트 이름 지정
TARGET_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vXco0waE_iBVhmXUqMe7O56KKSjY6bn4MiC3btoAPS8/edit"
TARGET_SHEET_NAME = "유튜브 요약"

# 설정
SHEET_CELL_LIMIT = 45000 
GPT_INPUT_LIMIT = 100000 

openai.api_key = OPENAI_API_KEY

# ==========================================
# 2. 구글 시트 연결 (URL 방식)
# ==========================================
def connect_google_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        if GCP_SA_KEY_STR:
            creds_dict = json.loads(GCP_SA_KEY_STR)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
            
        client = gspread.authorize(creds)
        
        try:
            # [핵심 수정] 이름 검색(open) 대신 URL로 직접 접속(open_by_url)
            print(f"📄 구글 시트 접속 중: {TARGET_SHEET_NAME}")
            spreadsheet = client.open_by_url(TARGET_SPREADSHEET_URL)
            
            # [핵심 수정] 0번째 시트가 아니라 지정된 이름("유튜브 요약")의 시트를 가져옴
            sheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
            
        except gspread.exceptions.WorksheetNotFound:
            # 만약 "유튜브 요약" 탭이 없으면 생성
            print(f"⚠️ '{TARGET_SHEET_NAME}' 시트가 없어 새로 생성합니다.")
            sheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows=100, cols=20)
            sheet.append_row(["채널명", "날짜", "제목", "스크립트", "GPT요약", "URL"])
            
        return sheet

    except Exception as e:
        print(f"[Error] 구글 시트 연결 실패: {str(e)}")
        print("💡 힌트: 서비스 계정 이메일이 해당 구글 시트에 '편집자'로 초대되어 있는지 확인해주세요.")
        raise e

# ==========================================
# 3. 영상 목록 수집 (테스트 모드: 2개만 수집)
# ==========================================
def get_all_videos(channel_id):
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        
        res = youtube.channels().list(id=channel_id, part="snippet,contentDetails").execute()
        
        if not res["items"]:
            print(f"⚠️ 채널 ID({channel_id})를 찾을 수 없습니다.")
            return [], "Unknown"

        channel_title = res["items"][0]["snippet"]["title"]
        playlist_id = res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        
        videos = []
        next_page_token = None
        
        print(f"📡 '{channel_title}' 영상 목록 조회 중... (테스트: 최대 2개)")
        
        while True:
            pl_res = youtube.playlistItems().list(
                playlistId=playlist_id,
                part="snippet",
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            
            for item in pl_res["items"]:
                video_id = item["snippet"]["resourceId"]["videoId"]
                title = item["snippet"]["title"]
                published_at = item["snippet"]["publishedAt"].split("T")[0]
                videos.append({"id": video_id, "title": title, "date": published_at})

                # [테스트용] 2개가 모이면 즉시 종료
                if len(videos) >= 2:
                    break
            
            if len(videos) >= 2:
                break

            next_page_token = pl_res.get("nextPageToken")
            if not next_page_token:
                break
                
        print(f"✅ 테스트를 위해 {len(videos)}개 영상만 수집했습니다.")
        return videos, channel_title
        
    except Exception as e:
        print(f"❌ 목록 조회 에러: {e}")
        return [], "Unknown"
    
# ==========================================
# 4. 자막 및 요약
# ==========================================
def get_transcript(video_id):
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['ko', 'en'])
        full_transcript = " ".join([item['text'] for item in transcript_list])
        return full_transcript
    except (TranscriptsDisabled, NoTranscriptFound):
        return None
    except Exception as e:
        return None

def summarize_text(text):
    if not text: return "자막 없음"
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        input_text = text[:GPT_INPUT_LIMIT]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "영상 내용을 빠짐없이 상세하게 요약해 주세요. 핵심 내용과 결론을 포함해야 합니다."},
                {"role": "user", "content": f"다음 내용을 요약해:\n\n{input_text}"}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"요약 실패: {str(e)}"

# ==========================================
# 5. 실행
# ==========================================
def main():
    print("🚀 유튜브 전체 수집기 시작")
    
    if not CHANNEL_IDS_STR:
        print("❌ Secrets에 'CHANNEL_ID'가 설정되지 않았습니다.")
        return

    sheet = connect_google_sheet()
    
    try:
        # URL 컬럼은 F열(6번째)
        existing_urls = set(sheet.col_values(6))
    except:
        existing_urls = set()

    target_channels = [id.strip() for id in CHANNEL_IDS_STR.split(",") if id.strip()]
    print(f"📋 타겟 채널: {target_channels}")

    for ch_id in target_channels:
        videos, channel_name = get_all_videos(ch_id)
        
        for video in reversed(videos):
            video_url = f"https://www.youtube.com/watch?v={video['id']}"
            
            if video_url in existing_urls:
                continue 
                
            print(f"▶ 처리 중 [{channel_name}]: {video['title']}")
            
            script = get_transcript(video['id'])
            
            if script:
                summary = summarize_text(script)
                
                if len(script) > SHEET_CELL_LIMIT:
                    saved_script = script[:SHEET_CELL_LIMIT] + "...(절삭)"
                else:
                    saved_script = script

                sheet.append_row([
                    channel_name,
                    video['date'],
                    video['title'],
                    saved_script,
                    summary,
                    video_url
                ])
                print(f"    ✅ 저장 완료")
                time.sleep(2)
            else:
                print(f"    ❌ 자막 없음 (건너뜀)")

if __name__ == "__main__":
    main()



# ==========================================
# 3. 영상 목록 수집
# ==========================================
# def get_all_videos(channel_id):
#     try:
#         youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        
#         res = youtube.channels().list(id=channel_id, part="snippet,contentDetails").execute()
        
#         if not res["items"]:
#             print(f"⚠️ 채널 ID({channel_id})를 찾을 수 없습니다.")
#             return [], "Unknown"

#         channel_title = res["items"][0]["snippet"]["title"]
#         playlist_id = res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        
#         videos = []
#         next_page_token = None
        
#         print(f"📡 '{channel_title}'의 전체 영상 목록 조회 중...")
        
#         while True:
#             pl_res = youtube.playlistItems().list(
#                 playlistId=playlist_id,
#                 part="snippet",
#                 maxResults=50,
#                 pageToken=next_page_token
#             ).execute()
            
#             for item in pl_res["items"]:
#                 video_id = item["snippet"]["resourceId"]["videoId"]
#                 title = item["snippet"]["title"]
#                 published_at = item["snippet"]["publishedAt"].split("T")[0]
#                 videos.append({"id": video_id, "title": title, "date": published_at})
            
#             next_page_token = pl_res.get("nextPageToken")
#             if not next_page_token:
#                 break
            
#             next_page_token = pl_res.get("nextPageToken")
#             # [테스트] 다음 페이지가 없거나, 수집된 영상이 2개 이상이면 종료
#             if not next_page_token or len(videos) >= 2: 
#                 break
                
#         print(f"✅ 총 {len(videos)}개 영상 발견")
#         return videos, channel_title
        
#     except Exception as e:
#         print(f"❌ 목록 조회 에러: {e}")
#         return [], "Unknown"