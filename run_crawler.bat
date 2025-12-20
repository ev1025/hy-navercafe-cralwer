@echo off

:: --- 스크립트가 있는 폴더로 이동 ---
cd "C:\Users\user\Desktop\project\naver_cafe_crawler"

:: --- 'logs' 라는 하위 폴더가 없으면 새로 생성 ---
if not exist "logs" mkdir "logs"

:: --- 오늘 날짜와 시간을 YYYY-MM-DD_HH-mm-ss 형식으로 변수에 저장 ---
FOR /F "tokens=*" %%i IN ('powershell -command "Get-Date -format 'yyyy-MM-dd_HH-mm-ss'"') DO SET NOW=%%i

:: --- 오늘 날짜로 로그 파일 전체 경로를 설정 ---
:: --- 👇 변수 이름을 TODAY에서 NOW로 수정했습니다 ---
SET LOG_FILE="logs\%NOW%.log"

:: --- 가상환경의 python.exe 실행 및 결과 저장 ---
"C:\Users\user\Desktop\project\naver_cafe_crawler\naver_cafe_crawler\Scripts\python.exe" "cafe_crawler.py" > %LOG_FILE% 2>&1