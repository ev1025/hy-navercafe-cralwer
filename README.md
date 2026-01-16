# Admission Data Intelligence: Naver Cafe & YouTube Automation

이 프로젝트는 입시 관련 커뮤니티와 유튜브 채널의 데이터를 자동으로 수집하고 인공지능을 통해 요약하여 구글 시트(Google Sheets)에 정리하는 자동화 시스템입니다.

---

## 📌 주요 기능 (Key Features)

### 1. 네이버 카페 크롤러 (`cafe_crawler.py`)

* **로그인 기반 수집**: `NAVER_COOKIE_STRING` 환경 변수를 통해 로그인 세션을 유지하며 멤버 공개 게시글과 댓글을 수집합니다.
* **멀티 카페 모니터링**: 수만휘, 토마스, 로물콘 등 지정된 카페의 특정 게시판 데이터를 동시에 수집할 수 있습니다.
* **비동기 고속 처리**: `aiohttp`와 `BeautifulSoup`을 사용하여 대량의 게시글 본문과 댓글을 빠르게 추출합니다.
* **구글 시트 연동**: 수집된 원본 데이터를 지정된 구글 시트의 '원본데이터' 워크시트에 자동으로 업로드합니다.
* **네이버 쿠키** : 네이버 부계정 로그인 정보를 활용하여 쿠키 세션을 장기간 유지하고, 카페 가입이 필요한 멤버 전용 게시글까지 안정적으로 수집하도록 설정합니다.
 
### 2. 유튜브 요약 스캐너 (`youtube_summary.py`)

* **쇼츠(Shorts) 원천 차단**: 채널 ID를 `UULF` 전용 플레이리스트로 변환하여 정보성이 높은 롱폼 영상만 선별적으로 수집합니다.
* **AI 자동 요약**: OpenAI의 **GPT-4o-mini** 모델을 활용해 영상 스크립트를 분석하고 핵심 내용을 불렛 포인트로 요약합니다.
* **프록시 서버 우회**: `Webshare` 프록시 설정을 적용하여 GitHub Actions 환경에서의 IP 차단 이슈를 방지하고 안정적으로 자막을 추출합니다.
* **자동 복구(A/S) 시스템**: 일시적인 오류로 요약이 실패한 항목을 마지막 단계에서 다시 찾아내어 재작업을 수행합니다.

---

## ⚙️ 설정 및 환경 변수 (GitHub Secrets)

프로젝트 정상 작동을 위해 GitHub 레포지토리의 `Settings > Secrets and variables > Actions`에 아래 환경 변수들을 반드시 등록해야 합니다.

| Secret Name | Description |
| --- | --- |
| `GCP_SA_KEY` | 구글 서비스 계정 JSON 키 데이터 (시트 접근용) |
| `NAVER_COOKIE_STRING` | 네이버 로그인 상태 유지를 위한 쿠키 문자열 |
| `GCP_API_KEY` | YouTube Data API v3 인증 키 |
| `OPENAI_API_KEY` | OpenAI API 호출을 위한 인증 키 |
| `PROXY_USERNAME` | Webshare 프록시 서버 사용자 이름 |
| `PROXY_PASSWORD` | Webshare 프록시 서버 비밀번호 |

---

## 📅 자동화 스케줄 (Workflows)

GitHub Actions를 통해 매일 정해진 시간에 클라우드 환경에서 자동으로 실행됩니다.

* **YouTube Summarizer**: 매일 한국 시간 기준 **오전 00:00** 실행 (`youtube_summary.yml`)
* **Naver Cafe Crawler**: 매일 한국 시간 기준 **오전 00:05** 실행 (`main.yml`)

---

## 🛠 기술 스택 (Tech Stack)

* **Language**: Python 3.10
* **AI Model**: OpenAI GPT-4o-mini
* **APIs**: YouTube Data API v3, Google Sheets API
* **Libraries**: `asyncio`, `aiohttp`, `gspread`, `youtube-transcript-api`, `BeautifulSoup`

---

**이 시스템은 입시 정보의 파편화를 방지하고, 효율적인 데이터 기반 의사결정을 돕기 위해 개발되었습니다.**
