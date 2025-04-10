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

# 영상 목록을 yt-dlp로 직접 가져오는 함수
def get_video_urls(channel_url, max_videos=None, start_date=None, end_date=None):
    print(f"[{datetime.now()}] 영상 목록 수집 시작: {channel_url}")
    cmd = [
        "yt-dlp",
        "--dump-json",
        "--skip-download",
        "--playlist-end", str(max_videos) if max_videos else "50",
    ]

    if start_date:
        cmd.extend(["--dateafter", start_date.replace("-", "")])
    if end_date:
        cmd.extend(["--datebefore", end_date.replace("-", "")])

    cmd.append(channel_url + "/videos")

    result = subprocess.run(cmd, capture_output=True, text=True)

    video_data = []
    for line in result.stdout.strip().split('\n'):
        try:
            data = json.loads(line)
            video_data.append({
                "title": data.get("title"),
                "url": data.get("webpage_url"),
                "published_at": data.get("upload_date")
            })
        except json.JSONDecodeError:
            print(f"[ERROR] JSON 파싱 실패: {line}")

    return video_data

# 자막 다운로드 및 처리
def get_subtitles(video_url, sub_lang="ko"):
    video_id = video_url.split("v=")[-1]
    subtitle_path = os.path.join(TEMP_FOLDER, f"{video_id}.{sub_lang}.vtt")

    if os.path.exists(subtitle_path):
        with open(subtitle_path, "r", encoding="utf-8") as file:
            return file.read()

    result = subprocess.run(
        ["yt-dlp", "--write-auto-sub", "--sub-lang", sub_lang, "--skip-download",
         "--output", TEMP_FOLDER + "/" + "%(id)s.%(ext)s", video_url],
        capture_output=True, text=True
    )

    if result.returncode == 0 and os.path.exists(subtitle_path):
        with open(subtitle_path, "r", encoding="utf-8") as file:
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

def process_video(video, sub_lang):
    url = video["url"]
    title = video["title"]
    print(f"[{datetime.now()}] 처리 시작: {url}")
    subtitles = get_subtitles(url, sub_lang)
    if subtitles:
        cleaned = clean_subtitles(subtitles)
        print(f"[{datetime.now()}] 처리 완료: {url}")
        return {
            "Title": title,
            "Video URL": url,
            "Published At": video.get("published_at"),
            "Subtitles": cleaned
        }
    print(f"[{datetime.now()}] 자막 없음 또는 처리 실패: {url}")
    return None

def collect_and_save_data(channel_url, sub_lang="ko", max_videos=None, start_date=None, end_date=None):
    video_data = get_video_urls(channel_url, max_videos, start_date, end_date)
    processed_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_video, video, sub_lang) for video in video_data]

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
        if optional and user_input.strip().lower() == "skip":
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
    start_date = get_valid_input("시작일 (YYYY-MM-DD 또는 'skip'): ", validate_date_format, optional=True)
    end_date = get_valid_input("종료일 (YYYY-MM-DD 또는 'skip'): ", validate_date_format, optional=True)

    collect_and_save_data(channel_url, subtitle_lang, max_videos, start_date, end_date)
