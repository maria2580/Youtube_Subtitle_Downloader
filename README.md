# **YouTube Subtitle Downloader**

## **프로젝트 개요**

YouTube Subtitle Downloader는 YouTube API 및 `yt-dlp`를 사용하여 특정 채널의 동영상 자막 데이터를 수집, 정리 및 저장하는 도구입니다. 이 도구는 연구, 데이터 분석, 언어 학습 및 AI 학습 데이터 생성 목적으로 설계되었습니다.

---

## **주요 기능**

- **YouTube 채널 데이터 수집**:
    - 동영상 제목, URL, 업로드 날짜 등을 수집
    - 최신순, 특정 기간 또는 제한된 수량으로 동영상 필터링 가능
- **자막 다운로드 및 전처리**:
    - 자동 생성된 자막 또는 제공된 자막 다운로드 (`yt-dlp` 사용)
    - 반복 단어/문장 제거 및 불필요한 태그 처리
- **CSV 파일로 저장**:
    - 동영상 메타데이터와 정리된 자막을 CSV 형식으로 저장

---

## **설치 및 실행**

### **1. 필수 조건**

- Python 3.8 이상
- 아래 Python 패키지 설치:
    
    ```bash
    pip install requests yt-dlp
    ```
    

### **2. 설치**

1. 이 프로젝트를 클론하거나 다운로드합니다.
    
    ```bash
    git clone https://github.com/your-repository/YouTube-Subtitle-Downloader.git
    cd YouTube-Subtitle-Downloader
    
    ```
    
2. 필요한 Python 패키지를 설치합니다.
    
    ```bash
    pip install -r requirements.txt
    
    ```
    

### **3. 실행**

```bash
python main.py

```

---

## **사용법**

1. 실행 후 YouTube 채널 URL, 자막 언어, 최대 동영상 개수, 시작일, 종료일 등을 입력합니다.
2. 입력한 조건에 맞는 동영상 자막 데이터를 수집하여 CSV 파일로 저장합니다.
3. 결과는 프로젝트 디렉토리에 생성된 `<채널 이름>_subtitles.csv` 파일에서 확인할 수 있습니다.

---

## **입력 예시**

```
YouTube 채널 URL을 입력하세요: https://www.youtube.com/@examplechannel
자막 언어(예: ko, en)를 입력하세요: ko
최대 비디오 갯수를 설정하십시오 (숫자 입력 또는 'skip'): 10
시작일자를 설정하십시오 (YYYY-MM-DD 형식 또는 'skip'): 2023-01-01
종료일자를 설정하십시오 (YYYY-MM-DD 형식 또는 'skip'): 2023-12-31

```

---

## **출력**

수집된 데이터는 CSV 파일로 저장되며, 파일에는 아래와 같은 정보가 포함됩니다:

- **Published At**: 동영상 업로드 날짜
- **Title**: 동영상 제목
- **Video URL**: 동영상 링크
- **Subtitles**: 정리된 자막 텍스트

---

## **구성 파일**

- `main.py`: 메인 실행 파일
- `cache/`: 캐시 데이터 저장 폴더
- `temp/`: 임시 파일 저장 폴더
- `requirements.txt`: 필요한 Python 패키지 리스트
- `README.md`: 프로젝트 설명서

---

## **주의 사항**

- **API 키**: Google API 키를 `main.py`의 `API_KEY` 변수에 설정해야 합니다.(참고 : https://brunch.co.kr/@mystoryg/156)
- **저작권**: 자막 데이터 사용 시 YouTube의 서비스 약관과 관련 법률을 준수해야 합니다.
- **IP 차단 위험**: `yt-dlp`를 통해 자막을 대량으로 다운로드할 경우, IP 차단이 발생할 수 있습니다. 적절한 속도로 다운로드를 진행하세요.

---

## **라이선스**

MIT 라이선스 하에 제공됩니다. LICENSE 파일을 참조하세요.

---

## **기여**

기여를 환영합니다! 버그 보고, 새로운 기능 제안 또는 코드 기여를 위해 Pull Request를 제출하세요.

---

## **문의**

프로젝트와 관련된 질문이나 문의는 아래 이메일로 연락 바랍니다.

- **이메일**: marin6670@gmail.com
