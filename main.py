import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import re
import os
import subprocess
from datetime import datetime

# 폴더 설정
TEMP_FOLDER = os.path.join(os.getcwd(), "temp")
if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)


# 영상 ID 목록을 가져오는 함수 (flat-playlist 활용)
def get_video_ids(channel_url, max_videos=None, start_date=None, end_date=None):
    print(f"[{datetime.now()}] 영상 ID 목록 수집 시작: {channel_url}")
    if start_date and end_date:
        # 검색 기반 수집
        search_query = f"{channel_url} before:{end_date} after:{start_date}"
        cmd = [
            "yt-dlp",
            f"ytsearch100:{search_query}",
            "--flat-playlist",
            "--print", "%(id)s"
        ]
    else:
        # 기본 채널 기반 수집
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--print", "%(id)s",
            channel_url + "/videos"
        ]

    if max_videos:
        cmd.extend(["--playlist-end", str(max_videos)])

    result = subprocess.run(cmd, capture_output=True, text=True)
    ids = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
    print(f"[{datetime.now()}] 수집 완료: 총 {len(ids)}개 ID")
    return ids


# 각 영상 ID에 대해 상세 정보 수집
def get_video_details(video_id):
    cmd = [
        "yt-dlp",
        f"https://www.youtube.com/watch?v={video_id}",
        "--dump-json",
        "--skip-download"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        data = json.loads(result.stdout)
        timestamp = data.get("upload_timestamp")
        if timestamp:
            published_at = datetime.utcfromtimestamp(timestamp).isoformat() + "Z"
        else:
            upload_date = data.get("upload_date")
            if upload_date:
                # upload_date: "YYYYMMDD" 형식 → ISO 형식으로 변환
                published_at = datetime.strptime(upload_date, "%Y%m%d").isoformat() + "Z"
            else:
                published_at = None
        return {
            "title": data.get("title"),
            "url": data.get("webpage_url"),
            "published_at": published_at
        }
    except json.JSONDecodeError:
        print(f"[ERROR] JSON 파싱 실패: {video_id}")
        return None


# 자막 다운로드 및 처리
def get_subtitles(video_url, sub_lang="ko"):
    video_id = video_url.split("v=")[-1]
    subtitle_path = os.path.join(TEMP_FOLDER, f"{video_id}.{sub_lang}.vtt")
    # 자동 생성된 자막은 파일명이 .auto.vtt로 나올 수 있음.
    subtitle_path_auto = os.path.join(TEMP_FOLDER, f"{video_id}.{sub_lang}.auto.vtt")

    if os.path.exists(subtitle_path):
        with open(subtitle_path, "r", encoding="utf-8") as file:
            return file.read()
    elif os.path.exists(subtitle_path_auto):
        with open(subtitle_path_auto, "r", encoding="utf-8") as file:
            return file.read()

    result = subprocess.run(
        ["yt-dlp", "--write-auto-sub", "--sub-lang", sub_lang, "--skip-download",
         "--output", os.path.join(TEMP_FOLDER, "%(id)s.%(ext)s"), video_url],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        if os.path.exists(subtitle_path):
            with open(subtitle_path, "r", encoding="utf-8") as file:
                return file.read()
        elif os.path.exists(subtitle_path_auto):
            with open(subtitle_path_auto, "r", encoding="utf-8") as file:
                return file.read()
    return ""


def clean_subtitles(subtitles):
    subtitles = subtitles.replace("\n", " ")
    subtitles = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d{3}>", "", subtitles)
    subtitles = re.sub(r"\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}", "", subtitles)
    subtitles = re.sub(r"align:[\w\-]+ position:\d+%", "", subtitles)
    subtitles = re.sub(r"<.*?>|\[.*?\]", "", subtitles)

    def remove_repeated_phrases(text):
        words = text.split()
        n = len(words)
        i = 0
        result = []
        while i < n:
            found_repeat = False
            for length in range(1, n - i):
                if length > 100:
                    continue
                segment = words[i:i + length]
                repetitions = 0
                for j in range(i, min(i + length * 3, n), length):
                    if words[j:j + length] == segment:
                        repetitions += 1
                    else:
                        break
                if repetitions >= 3:
                    found_repeat = True
                    i += length * repetitions
                    result.extend(segment)
                    break
            if not found_repeat:
                result.append(words[i])
                i += 1
        return " ".join(result)

    cleaned_subtitles = remove_repeated_phrases(subtitles)
    cleaned_subtitles = re.sub(r"\s{2,}", " ", cleaned_subtitles)
    return cleaned_subtitles.strip()


def process_video(video_id, sub_lang, start_date=None, end_date=None):
    print(f"[{datetime.now()}] 조회 및 처리 시작: {video_id}")
    video = get_video_details(video_id)
    if not video:
        print(f"[{datetime.now()}] 영상 정보 없음: {video_id}")
        return None

    # 날짜 필터링 (get_video_ids에서 필터링하지 못한 영상도 보완)
    published_at = video.get("published_at")
    if published_at and (start_date or end_date):
        video_date = datetime.fromisoformat(published_at.replace("Z", ""))
        if start_date and video_date < datetime.fromisoformat(start_date):
            return None
        if end_date and video_date > datetime.fromisoformat(end_date):
            return None

    url = video["url"]
    title = video["title"]
    subtitles = get_subtitles(url, sub_lang)
    if subtitles:
        cleaned = clean_subtitles(subtitles)
        print(f"[{datetime.now()}] 처리 완료: {video_id}")
        return {
            "Title": title,
            "Video URL": url,
            "Published At": published_at,
            "Subtitles": cleaned
        }
    print(f"[{datetime.now()}] 자막 없음 또는 처리 실패: {video_id}")
    return None


def collect_and_save_data(channel_url, sub_lang="ko", max_videos=None, start_date=None, end_date=None):
    video_ids = get_video_ids(channel_url, max_videos, start_date, end_date)
    processed_data = []

    # 시스템의 논리 프로세서 수 중 1개를 남겨두도록 설정 (최소 1개)
    num_workers = max((os.cpu_count() or 1) - 1, 1)
    print(f"[{datetime.now()}] 스레드 풀 생성: max_workers={num_workers}")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_video, vid, sub_lang, start_date, end_date) for vid in video_ids]
        for future in as_completed(futures):
            result = future.result()
            if result:
                processed_data.append(result)

    channel_name = channel_url.rstrip('/').split("/")[-1]
    output_file = f"{channel_name}_subtitles.csv"
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        fieldnames = ["Published At", "Title", "Video URL", "Subtitles"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_data)
    print(f"Data saved to: {output_file}")


def get_valid_input(prompt, validation_func=None, optional=False):
    while True:
        user_input = input(prompt)
        if optional and user_input.strip().lower() in ["skip", "s"]:
            return None
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


if __name__ == "__main__":
    channel_url = input("YouTube 채널 URL 또는 핸들을 입력하세요: ")
    subtitle_lang = input("자막 언어(ko, en 등)를 입력하세요: ")
    max_videos = get_valid_input("최대 영상 수(숫자 또는 'skip'): ", validate_positive_integer, optional=True)
    max_videos = int(max_videos) if max_videos else None
    start_date = get_valid_input("시작일 (YYYY-MM-DD 또는 'skip:s'): ", validate_date_format, optional=True)
    end_date = get_valid_input("종료일 (YYYY-MM-DD 또는 'skip:s'): ", validate_date_format, optional=True)
    collect_and_save_data(channel_url, subtitle_lang, max_videos, start_date, end_date)
# 예: https://www.youtube.com/@sbsnews8
