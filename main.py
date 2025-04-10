from concurrent.futures import ThreadPoolExecutor, as_completed

import csv
import re
import requests
import os
import subprocess
import json
import datetime

# Google API 설정
API_KEY = "AIzaSyDam5Y2UzWVmN09ODpUxDfxCmi1q6jJveo"  # 여기에 API 키 입력
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"


# 임시 폴더 및 캐시 폴더 설정
TEMP_FOLDER = os.path.join(os.getcwd(), "temp")
CACHE_FOLDER = os.path.join(os.getcwd(), "cache")


# 폴더 생성
for folder in [TEMP_FOLDER, CACHE_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)


# 캐시 파일 경로
CACHE_FILE = os.path.join(CACHE_FOLDER, "channel_cache.json")


# 캐시 초기화
if not os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f)


def load_cache():
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(cache_data):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=4)


# 채널 URL에서 채널 ID 추출 (캐시 활용)
def get_channel_id(channel_url):
    cache = load_cache()

    # 캐시에 핸들 또는 URL 저장 여부 확인
    if channel_url in cache:
        return cache[channel_url]

    if "youtube.com/channel/" in channel_url:
        channel_id = channel_url.split("youtube.com/channel/")[-1]
    elif "youtube.com/c/" in channel_url:
        username = channel_url.split("youtube.com/c/")[-1]
        channel_id = get_channel_id_from_username(username)
    elif "youtube.com/@" in channel_url:
        handle = channel_url.split("youtube.com/@")[-1]
        channel_id = get_channel_id_from_handle(handle)
    else:
        raise ValueError("URL 형식을 확인하세요. 지원되지 않는 URL입니다.")

    # 캐시에 저장
    cache[channel_url] = channel_id
    save_cache(cache)
    return channel_id


def get_channel_id_from_username(username):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=id&forUsername={username}&key={API_KEY}"
    response = requests.get(url).json()
    if "items" in response and response["items"]:
        return response["items"][0]["id"]
    raise ValueError("사용자 이름으로 채널 ID를 찾을 수 없습니다.")


def get_channel_id_from_handle(handle):
    url = f"{YOUTUBE_BASE_URL}/search?part=snippet&type=channel&q=@{handle}&key={API_KEY}"
    response = requests.get(url).json()
    if "items" in response and response["items"]:
        return response["items"][0]["id"]["channelId"]
    raise ValueError("핸들에서 채널 ID를 찾을 수 없습니다.")


def get_video_urls(api_key, channel_id, max_videos=None, start_date=None, end_date=None):
    """
    채널의 비디오 URL과 관련 데이터를 가져옵니다.

    Parameters:
        api_key (str): YouTube API 키
        channel_id (str): 채널 ID
        max_videos (int, optional): 가져올 영상 최대 개수
        start_date (str, optional): 시작 날짜 (ISO 8601 형식, YYYY-MM-DD)
        end_date (str, optional): 종료 날짜 (ISO 8601 형식, YYYY-MM-DD)

    Returns:
        list: 제목, URL, 업로드 날짜를 포함하는 비디오 데이터
    """
    video_data = []  # 결과를 저장할 리스트
    next_page_token = ""
    video_count = 0

    # 날짜를 datetime 객체로 변환
    start_date = datetime.fromisoformat(start_date) if start_date else None
    end_date = datetime.fromisoformat(end_date) if end_date else None

    while True:
        url = (
            f"{YOUTUBE_BASE_URL}/search?"
            f"key={api_key}&channelId={channel_id}&part=snippet&type=video&maxResults=50"
            f"&order=date"  # 최신순 정렬 추가
            f"&pageToken={next_page_token}"
        )
        response = requests.get(url).json()

        if "items" in response:
            for item in response["items"]:
                video_id = item["id"]["videoId"]
                title = item["snippet"]["title"]
                published_at = item["snippet"]["publishedAt"]

                # 업로드 날짜 필터링
                published_date = datetime.fromisoformat(published_at.replace("Z", ""))
                if start_date and published_date < start_date:
                    continue
                if end_date and published_date > end_date:
                    continue

                video_data.append({
                    "title": title,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "published_at": published_at
                })

                # 최대 개수 제한
                video_count += 1
                if max_videos and video_count >= max_videos:
                    return video_data

        # 다음 페이지로 이동
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_data

# 영상 자막 다운로드 (캐시 활용)
def get_subtitles(video_url, sub_lang="ko"):
    video_id = video_url.split("v=")[-1]
    subtitle_path = os.path.join(TEMP_FOLDER, f"{video_id}.{sub_lang}.vtt")

    # 캐시된 파일이 존재하면 스킵
    if os.path.exists(subtitle_path):
        with open(subtitle_path, "r", encoding="utf-8") as file:
            return file.read()

    # yt-dlp로 자막 다운로드
    result = subprocess.run(
        [
            "yt-dlp",
            "--write-auto-sub",
            "--sub-lang", sub_lang,
            "--skip-download",
            "--output", TEMP_FOLDER + "/" + "%(id)s.%(ext)s",
            video_url,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0 and os.path.exists(subtitle_path):
        with open(subtitle_path, "r", encoding="utf-8") as file:
            return file.read()

    return ""


def clean_subtitles(subtitles):
    # 1. 줄바꿈 제거
    subtitles = subtitles.replace("\n", " ")

    # 2. 시간 및 위치 정보 제거
    subtitles = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d{3}>", "", subtitles)
    subtitles = re.sub(r"\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}", "", subtitles)
    subtitles = re.sub(r"align:[\w\-]+ position:\d+%", "", subtitles)

    # 3. HTML 태그 및 기타 불필요한 태그 제거
    subtitles = re.sub(r"<.*?>|\[.*?\]", "", subtitles)

    # 4. 단어 기반 반복 구문 탐지 및 제거
    def remove_repeated_phrases(text):
        words = text.split()
        n = len(words)
        i = 0
        result = []

        while i < n:
            found_repeat = False
            # 최대 반복 길이를 탐색 (1음절부터 최대 남은 길이까지)
            for length in range(1, n - i):
                if length>100:
                    continue;
                segment = words[i:i + length]  # 현재 검사 중인 세그먼트
                repetitions = 0

                # 다음 3n개 단어에서만 탐색
                for j in range(i, min(i + length * 3, n), length):
                    if words[j:j + length] == segment:
                        repetitions += 1
                    else:
                        break

                # 3번 반복되면 제거
                if repetitions >= 3:
                    found_repeat = True
                    i += length * repetitions  # 반복된 부분 건너뛰기
                    result.extend(segment)  # 중복 중 하나만 결과에 추가
                    break

            if not found_repeat:  # 반복을 찾지 못한 경우
                result.append(words[i])
                i += 1

        return " ".join(result)

    # 반복 구문 제거
    cleaned_subtitles = remove_repeated_phrases(subtitles)

    # 5. 여러 공백을 단일 공백으로 압축
    cleaned_subtitles = re.sub(r"\s{2,}", " ", cleaned_subtitles)

    return cleaned_subtitles.strip()


# 스레드 풀을 활용한 데이터 수집
def process_video(video_url, sub_lang):
    # 작업 시작 로그
    print(datetime.now().strftime("%H:%M:%S"), "Starting processing: ", video_url)
    try:
        subtitles = get_subtitles(video_url, sub_lang)
        if subtitles:
            cleaned_subtitles = clean_subtitles(subtitles)
            # 작업 완료 로그
            print(datetime.now().strftime("%H:%M:%S"), "Finished processing: ", video_url)
            return {"Video URL": video_url, "Subtitles": cleaned_subtitles}
    except Exception as e:

        print(datetime.now().strftime("%H:%M:%S"), f"Error processing {video_url}: {e}")
    return None


def collect_and_save_data(channel_url, sub_lang="ko",max_videos=None,start_date=None,end_date=None):
    try:
        channel_id = get_channel_id(channel_url)
        video_data = get_video_urls(API_KEY, channel_id, max_videos, start_date, end_date)

        processed_data = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_video = {executor.submit(process_video, video["url"], sub_lang): video for video in video_data}

            for future in as_completed(future_to_video):
                result = future.result()
                if result:
                    video_info = future_to_video[future]
                    processed_data.append({
                        "Title": video_info["title"],
                        "Video URL": result["Video URL"],
                        "Subtitles": result["Subtitles"],
                        "Published At": video_info["published_at"]
                    })

        channel_name = channel_url.split("/")[-1]
        output_file = f"{channel_name}_subtitles.csv"
        with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
            fieldnames = ["Published At", "Title", "Video URL", "Subtitles"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_data)

        print(f"Data saved to: {output_file}")
    except Exception as e:
        print(f"오류 발생: {e}")


if __name__ == "__main__":
    def get_valid_input(prompt, validation_func=None, optional=False):
        """
        사용자 입력을 검증하고 유효하지 않은 경우 다시 입력 요청.

        Parameters:
            prompt (str): 입력 요청 메시지
            validation_func (function, optional): 입력 검증 함수
            optional (bool): 입력 스킵 가능 여부

        Returns:
            str: 유효한 사용자 입력
        """
        while True:
            user_input = input(prompt)
            if optional and user_input.strip().lower() == "skip":
                return None  # 스킵 처리
            if validation_func is None or validation_func(user_input):
                return user_input
            print("잘못된 입력입니다. 다시 시도해주세요.")


    def validate_positive_integer(value):
        return value.isdigit() and int(value) > 0


    def validate_date_format(value):
        try:
            datetime.fromisoformat(value)
            return True
        except ValueError:
            return False


    # 사용자 입력 받기
    channel_url = input("YouTube 채널 URL을 입력하세요: ")
    subtitle_lang = input("자막 언어(예: ko, en)를 입력하세요: ")

    # 최대 비디오 갯수 입력
    max_videos = get_valid_input(
        "최대 비디오 갯수를 설정하십시오 (숫자 입력 또는 'skip'): ",
        validation_func=validate_positive_integer,
        optional=True
    )
    max_videos = int(max_videos) if max_videos else None

    # 시작일자 입력
    start_date = get_valid_input(
        "시작일자를 설정하십시오 (YYYY-MM-DD 형식 또는 'skip'): ",
        validation_func=validate_date_format,
        optional=True
    )

    # 종료일자 입력
    end_date = get_valid_input(
        "종료일자를 설정하십시오 (YYYY-MM-DD 형식 또는 'skip'): ",
        validation_func=validate_date_format,
        optional=True
    )

    # 데이터 수집 및 저장
    collect_and_save_data(channel_url, subtitle_lang, max_videos, start_date, end_date)