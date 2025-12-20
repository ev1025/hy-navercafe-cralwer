import os
import sys
import time
from typing import Optional

import pyperclip
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


LOGIN_URL = "https://nid.naver.com/nidlogin.login"

def paste_with_clipboard(driver, element, text, modifier_key) -> bool:
    """클립보드에 text를 넣고, element에 붙여넣기. 성공 여부 반환."""
    try:
        element.click()
        element.clear()
        time.sleep(0.1)

        pyperclip.copy(text)           # OS 클립보드에 직접 복사
        element.send_keys(modifier_key, 'v')  # 붙여넣기
        time.sleep(0.15)

        # 값 검증 (붙여넣기 실패 시 보정)
        val = element.get_attribute("value") or ""
        if val.strip() != text:
            # 실패 보정: JS로 직접 주입
            driver.execute_script("arguments[0].value = arguments[1];", element, text)
            time.sleep(0.05)
            val = element.get_attribute("value") or ""
        return val.strip() == text
    except Exception:
        return False

def get_naver_cookies(headless: bool = False) -> Optional[str]:
    """
    네이버 로그인 후 쿠키 문자열 반환. 실패 시 None.
    .env의 NAVER_ID, NAVER_PW 사용.
    """
    load_dotenv()
    NAVER_ID = os.getenv("NAVER_ID")
    NAVER_PW = os.getenv("NAVER_PW")
    if not NAVER_ID or not NAVER_PW:
        print("환경변수 NAVER_ID 또는 NAVER_PW가 설정되지 않았습니다. .env를 확인하세요.")
        return None

    # OS별 붙여넣기 조합 키
    if sys.platform == "darwin":
        modifier_key = Keys.COMMAND   # macOS: ⌘+V
    else:
        modifier_key = Keys.CONTROL   # Windows/Linux: Ctrl+V

    driver = None
    try:
        options = webdriver.ChromeOptions()

        options.add_argument("--headless")
        options.add_argument("--window-size=1280,960")
        

        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 15)

        # 1) 로그인 페이지
        driver.get(LOGIN_URL)

        # 2) 요소 대기
        id_input = wait.until(EC.presence_of_element_located((By.ID, "id")))
        pw_input = wait.until(EC.presence_of_element_located((By.ID, "pw")))

        # 3) 클립보드 붙여넣기(실패 시 JS 주입 보정)
        ok_id = paste_with_clipboard(driver, id_input, NAVER_ID, modifier_key)
        ok_pw = paste_with_clipboard(driver, pw_input, NAVER_PW, modifier_key)
        if not (ok_id and ok_pw):
            print("경고: 일부 필드에 붙여넣기 실패가 감지되어 JS 보정이 적용되었습니다.")

        # 4) 로그인 버튼 클릭(여러 후보)
        print("로그인을 시도합니다...")
        clicked = False
        for by, sel in [
            (By.ID, "log.login"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[contains(., '로그인')]"),
        ]:
            try:
                btn = wait.until(EC.element_to_be_clickable((by, sel)))
                btn.click()
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            pw_input.send_keys(Keys.ENTER)

        # 5) 전환 대기 (캡차/2단계일 수 있음: 창에서 처리)
        time.sleep(2)
        if "nidlogin.login" in driver.current_url:
            print("추가 인증(캡차/2단계)이 필요합니다. 브라우저 창에서 완료 후 다시 시도하세요.")
            # 여기서 수동 인증을 마치면 URL이 바뀜. 감지 루프 120초.
            end = time.time() + 120
            while time.time() < end and "nidlogin.login" in driver.current_url:
                time.sleep(0.5)

        # 6) 최종 확인
        if "nidlogin.login" in driver.current_url:
            print("로그인 실패 또는 추가 인증 미완료입니다.")
            return None

        print("로그인 성공! 쿠키를 추출합니다...")
        cookies = driver.get_cookies()
        cookie_string = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        return cookie_string

    except Exception as e:
        print(f"오류: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

if __name__ == "__main__":
    cookie = get_naver_cookies(headless=True)  # 캡차/2단계 때문에 창 표시 권장
    if cookie:
        print("\n--- 추출된 쿠키 (이 값을 헤더에 사용하세요) ---")
        print(cookie)
