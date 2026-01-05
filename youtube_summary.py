import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials  # [변경] oauth2client 대신 최신 라이브러리 사용
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

# [설정] 구글 시트 URL (편집 모드 /edit 로 끝나는 주소)
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
    # [변경] 최신 스코프 및 인증 방식 적용
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        print("🔑 구글 인증(New Version) 시도 중...")
        
        # 1. 서비스 계정 키 로드 (JSON 문자열 or 파일)
        if GCP_SA_KEY_STR:
            creds_dict = json.loads(GCP_SA_KEY_STR)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
            
        # 2. gspread 연결
        client = gspread.authorize(creds)
        
        # 3. 시트 열기
        try:
            print(f"📄 구글 시트 접속 중: {TARGET_SHEET_NAME}")
            spreadsheet = client.open_by_url(TARGET_SPREADSHEET_URL)
            sheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
            
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️ '{TARGET_SHEET_NAME}' 시트가 없어 새로 생성합니다.")
            sheet = spreadsheet.add_worksheet(title=TARGET_SHEET_NAME, rows=100, cols=20)
        
        # 4. 헤더 확인
        if not sheet.row_values(1):
            print("📝 헤더(첫 줄)를 생성합니다.")
            sheet.append_row(["채널명", "날짜", "제목", "스크립트", "GPT요약", "URL"])
            
        return sheet

    except Exception as e:
        print(f"[Error] 구글 시트 연결 실패: {str(e)}")
        print("💡 힌트: 서비스 계정 이메일이 해당 구글 시트에 '편집자'로 초대되어 있는지 확인해주세요.")
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
                published_at = item["snippet"]["publishedAt"].split("T")[0]
                videos.append({"id": video_id, "title": title, "date": published_at})

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
# 4. 자막 및 요약 (신버전 API 대응)
# ==========================================
def get_transcript(video_id):
    try:
        # [변경] v1.x 이상에서는 객체를 생성(Instantiate)해서 사용해야 합니다.
        yt = YouTubeTranscriptApi()
        
        # 1. 자막 목록 가져오기
        # 주의: 신버전에서도 list_transcripts가 없다면 yt.get_transcript(video_id)를 바로 써야할 수 있으나
        # 대부분의 경우 list_transcripts 메서드를 제공합니다.
        try:
            transcript_list = yt.list_transcripts(video_id)
        except AttributeError:
             # 만약 진짜 최신 버전에서 메서드 이름이 바뀌었다면 fetch fallback
             # (일부 버전에서는 yt.fetch(video_id)로 대체될 수 있음)
             transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        transcript = None
        
        # 2. 수동 자막 우선 (ko, en)
        try:
            transcript = transcript_list.find_manually_created_transcript(['ko', 'ko-KR', 'en', 'en-US'])
        except:
            pass

        # 3. 자동 자막 차선
        if not transcript:
            try:
                transcript = transcript_list.find_generated_transcript(['ko', 'ko-KR', 'en', 'en-US'])
            except:
                pass
        
        # 4. 번역 시도 (Fallback)
        if not transcript:
            try:
                transcript = next(iter(transcript_list))
                if not transcript.language_code.startswith('ko'):
                    print(f"  - ({transcript.language_code}) 자막 발견 -> 한국어 번역 시도")
                    transcript = transcript.translate('ko')
            except:
                return None

        # 5. 데이터 추출
        transcript_data = transcript.fetch()
        text_list = []
        for entry in transcript_data:
            if isinstance(entry, dict) and 'text' in entry:
                text_list.append(entry['text'])
            elif hasattr(entry, 'text'):
                text_list.append(entry.text)
        
        return " ".join(text_list)

    except Exception as e:
        # 에러 메시지에 따라 로그 출력
        print(f"  ❌ 자막 가져오기 실패: {e}")
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
    print("🚀 유튜브 전체 수집기 시작 (New Version)")
    
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