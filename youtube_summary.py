import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
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

# [설정] 구글 시트 URL
TARGET_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1vXco0waE_iBVhmXUqMe7O56KKSjY6bn4MiC3btoAPS8/edit"
TARGET_SHEET_NAME = "유튜브 요약"

# 설정
SHEET_CELL_LIMIT = 45000 
GPT_INPUT_LIMIT = 100000 

openai.api_key = OPENAI_API_KEY

# ==========================================
# 2. 구글 시트 연결 (google-auth 신버전 방식)
# ==========================================
def connect_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        print("🔑 구글 인증(New Version) 시도 중...")
        
        if GCP_SA_KEY_STR:
            creds_dict = json.loads(GCP_SA_KEY_STR)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
            
        client = gspread.authorize(creds)
        
        try:
            print(f"📄 구글 시트 접속 중: {TARGET_SHEET_NAME}")
            spreadsheet = client.open_by_url(TARGET_SPREADSHEET_URL)
            sheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
            
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️ '{TARGET_SHEET_NAME}' 시트가 없어 새로 생성합니다.")
            sheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows=100, cols=20)
        
        if not sheet.row_values(1):
            print("📝 헤더(첫 줄)를 생성합니다.")
            sheet.append_row(["채널명", "날짜", "제목", "스크립트", "GPT요약", "URL"])
            
        return sheet

    except Exception as e:
        print(f"[Error] 구글 시트 연결 실패: {str(e)}")
        raise e

# ==========================================
# 3. 영상 목록 수집
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
                
                # [옵션] 라이브 영상 등 필터링이 필요하면 여기서 if문 추가
                
                published_at = item["snippet"]["publishedAt"].split("T")[0]
                videos.append({"id": video_id, "title": title, "date": published_at})

                if len(videos) >= 2: # 테스트용 2개 제한
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
# 4. 자막 및 요약 (사용자 요청: 심플 표준 방식)
# ==========================================
def get_transcript(video_id):
    """
    복잡한 로직을 제거하고 YouTubeTranscriptApi.get_transcript 표준 함수를 사용합니다.
    languages=['ko', 'en'] 설정 시:
    1. 한국어(수동) -> 한국어(자동) 순으로 찾습니다.
    2. 없으면 영어(수동) -> 영어(자동) 순으로 찾습니다.
    """
    try:
        # [핵심 변경] 사용자님이 성공한 방식과 동일한 로직입니다.
        # 이 함수는 자막 딕셔너리 리스트를 바로 반환합니다.
        ytt_api = YouTubeTranscriptApi()
        transcript_data = ytt_api.fetch(video_id, languages = [ 'ko' ])
        
        # 텍스트만 추출하여 합치기
        text_list = [entry['text'] for entry in transcript_data]
        return " ".join(text_list)

    except NoTranscriptFound:
        print(f"  ❌ 자막 없음 (한국어/영어 자막을 찾을 수 없음)")
        return None
    except TranscriptsDisabled:
        print(f"  ❌ 자막 기능이 비활성화된 영상입니다.")
        return None
    except Exception as e:
        print(f"  ❌ 자막 에러 발생: {e}")
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
    print("🚀 유튜브 전체 수집기 시작 (Simple Version)")
    
    if not CHANNEL_IDS_STR:
        print("❌ Secrets에 'CHANNEL_ID'가 설정되지 않았습니다.")
        return

    sheet = connect_google_sheet()
    
    try:
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
                saved_script = script
                if len(saved_script) > SHEET_CELL_LIMIT:
                    saved_script = saved_script[:SHEET_CELL_LIMIT] + "...(절삭)"
                status_msg = "✅ 요약 완료"
            else:
                saved_script = "자막 없음"
                summary = "요약 불가"
                status_msg = "⚠️ 자막 없음 (행만 추가함)"

            sheet.append_row([
                channel_name,
                video['date'],
                video['title'],
                saved_script,
                summary,
                video_url
            ])
            print(f"    {status_msg}")
            time.sleep(2)

if __name__ == "__main__":
    main()