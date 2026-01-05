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

# [설정] 구글 시트 URL (편집 모드 /edit 로 끝나는 주소)
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
        print("💡 힌트: 서비스 계정 이메일이 해당 구글 시트에 '편집자'로 초대되어 있는지 확인해주세요.")
        raise e

# ==========================================
# 3. 영상 목록 수집 (테스트 모드: 2개만 수집)
# ==========================================
def get_all_videos(channel_id):
    try:
        # 제목 추출은 블로그의 BeautifulSoup 방식보다 이 공식 API 방식이 훨씬 안정적이고 정확합니다.
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
# 4. 자막 및 요약 (블로그 내용 반영 + 기능 강화)
# ==========================================
def get_transcript(video_id):
    """
    [블로그 반영 사항]
    1. 수동 자막(find_manually_created_transcript) 우선 시도
    2. 실패 시 자동 자막(find_generated_transcript) 시도
    3. 데이터 추출 시 딕셔너리/객체 타입 안전하게 확인 (hasattr)
    
    [기존 기능 유지]
    4. 외국어만 있을 경우 한국어로 번역 (translate)
    """
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        transcript = None
        
        # 1. 수동 생성 자막 우선 검색 (퀄리티가 더 좋음)
        try:
            transcript = transcript_list.find_manually_created_transcript(['ko', 'ko-KR', 'en', 'en-US'])
        except:
            pass

        # 2. 수동이 없으면 자동 생성 자막 검색
        if not transcript:
            try:
                transcript = transcript_list.find_generated_transcript(['ko', 'ko-KR', 'en', 'en-US'])
            except:
                pass
        
        # 3. 그래도 없으면 "아무 언어"나 가져와서 "한국어 번역" 시도
        if not transcript:
            try:
                transcript = next(iter(transcript_list)) # 첫 번째 자막 (보통 원어)
                # 한국어가 아니면 번역
                if not transcript.language_code.startswith('ko'):
                    print(f"  - ({transcript.language_code}) 자막 발견 -> 한국어 번역 시도")
                    transcript = transcript.translate('ko')
            except:
                print(f"  ❌ 사용 가능한 자막 없음")
                return None

        # 4. 자막 데이터 안전하게 추출 (블로그 로직 반영)
        transcript_data = transcript.fetch()
        text_list = []
        
        for entry in transcript_data:
            # 딕셔너리 형태인 경우
            if isinstance(entry, dict) and 'text' in entry:
                text_list.append(entry['text'])
            # 객체 형태인 경우 (라이브러리 버전에 따라 다를 수 있음)
            elif hasattr(entry, 'text'):
                text_list.append(entry.text)
        
        full_transcript = " ".join(text_list)
        return full_transcript

    except TranscriptsDisabled:
        print(f"  ❌ 자막 기능이 비활성화된 영상입니다.")
        return None
    except NoTranscriptFound:
        print(f"  ❌ 자막을 찾을 수 없습니다.")
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
    print("🚀 유튜브 전체 수집기 시작")
    
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
                saved_script = "자막 없음 (라이브 직후 또는 자막 미제공)"
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