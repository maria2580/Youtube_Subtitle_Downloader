import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import csv
import re
import os
import subprocess
from datetime import datetime
import sys
import time
import pickle
import hashlib
import logging
import queue
import threading
import requests
from functools import lru_cache

# 로깅 설정
LOG_FOLDER = os.path.join(os.getcwd(), "logs")
TEMP_FOLDER = os.path.join(os.getcwd(), "temp")
CACHE_FOLDER = os.path.join(os.getcwd(), "cache")
RESULT_FOLDER = os.path.join(os.getcwd(), "result")

for folder in [LOG_FOLDER, TEMP_FOLDER, CACHE_FOLDER, RESULT_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_FOLDER, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"),
                            encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# 캐시 관리
class Cache:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def get_cache_path(self, key):
        hashed_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hashed_key}.pkl")

    def get(self, key):
        cache_path = self.get_cache_path(key)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"캐시 로딩 실패: {e}")
                return None
        return None
    def set(self, key, value):
        cache_path = self.get_cache_path(key)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(value, f)
            return True
        except Exception as e:
            logger.warning(f"캐시 저장 실패: {e}")
            return False


# 인스턴스 생성
video_cache = Cache(os.path.join(CACHE_FOLDER, "videos"))
subtitle_cache = Cache(os.path.join(CACHE_FOLDER, "subtitles"))


# 요청 처리 및 재시도 로직
def execute_command(cmd, retries=3, backoff_factor=1.5):
    attempt = 0
    last_error = None

    while attempt < retries:
        try:
            logger.debug(f"명령 실행: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            # ─── 멤버 전용 에러 감지 ───────────────────────────────────────
            stderr = result.stderr or ""
            if result.returncode != 0 and "available to this channel's members" in stderr or "Join this channel to get access" in stderr or "members-only content" in stderr:
                logger.warning(f"멤버 전용 영상, 더 이상 재시도하지 않고 스킵합니다.")
                # 강제로 재시도 횟수를 다 소진시켜 바로 함수 종료
                attempt = retries
                break
            # ──────────────────────────────────────────────────────────

            if result.returncode == 0:
                return result
            last_error = f"반환 코드: {result.returncode}, stderr: {stderr}"

        except subprocess.TimeoutExpired:
            last_error = "명령 실행 시간 초과"
        except Exception as e:
            last_error = str(e)

        # 재시도
        attempt += 1
        wait_time = backoff_factor ** attempt
        logger.warning(f"명령 실패 ({attempt}/{retries}), {wait_time:.1f}초 후 재시도. 오류: {last_error}")
        time.sleep(wait_time)

    raise Exception(f"최대 재시도 횟수 초과: {last_error}")


# 영상 ID 목록을 가져오는 함수 (성능 개선)
def get_video_ids(channel_url, max_videos=None, start_date=None, end_date=None):
    cache_key = f"{channel_url}_{max_videos}_{start_date}_{end_date}"
    cached_ids = video_cache.get(cache_key)
    if cached_ids:
        logger.info(f"캐시에서 {len(cached_ids)}개의 영상 ID 로드")
        return cached_ids

    logger.info(f"영상 ID 목록 수집 시작: {channel_url}")

    if start_date and end_date:
        # 검색 기반 수집
        search_query = f"{channel_url} before:{end_date} after:{start_date}"
        cmd = [
            "yt-dlp",
            f"ytsearch100:{search_query}",
            "--flat-playlist",
            "--print", "%(id)s",
            "--socket-timeout", "30"
        ]
    else:
        # 기본 채널 기반 수집
        cmd = [
            "yt-dlp",
            "--no-warnings",
            "--flat-playlist",
            "--print", "%(id)s",
            "--socket-timeout", "30",
            channel_url + "/videos"
        ]

    if max_videos:
        cmd.extend(["--playlist-end", str(max_videos)])

    try:
        result = execute_command(cmd)
        ids = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
        logger.info(f"수집 완료: 총 {len(ids)}개 ID")

        # 캐시에 저장
        video_cache.set(cache_key, ids)
        return ids
    except Exception as e:
        logger.error(f"영상 ID 수집 실패: {e}")
        return []


# 병렬 실행 관리를 위한 작업 큐
result_queue = queue.Queue()
progress_lock = threading.Lock()
processed_count = 0
total_count = 0


@lru_cache(maxsize=None)
def get_video_details(video_id):
    # 캐시 확인
    cache_key = f"details_{video_id}"
    cached_details = video_cache.get(cache_key)
    if cached_details:
        return cached_details

    cmd = [
        "yt-dlp",
        "--no-warnings",
        f"https://www.youtube.com/watch?v={video_id}",
        "--dump-json",
        "--skip-download",
        "--socket-timeout", "30",
        "--retries", "3"
    ]

    try:
        result = execute_command(cmd)
        data = json.loads(result.stdout)

        timestamp = data.get("upload_timestamp")
        if timestamp:
            published_at = datetime.utcfromtimestamp(timestamp).isoformat() + "Z"
        else:
            upload_date = data.get("upload_date")
            if upload_date:
                published_at = datetime.strptime(upload_date, "%Y%m%d").isoformat() + "Z"
            else:
                published_at = None

        details = {
            "title": data.get("title"),
            "url": data.get("webpage_url"),
            "published_at": published_at
        }

        # 캐시에 저장
        video_cache.set(cache_key, details)
        return details

    except Exception as e:
        logger.error(f"영상 정보 가져오기 실패 ({video_id}): {e}")
        return None


def get_subtitles(video_url, sub_lang="ko"):
    video_id = video_url.split("v=")[-1]
    subtitle_path = os.path.join(TEMP_FOLDER, f"{video_id}.{sub_lang}.vtt")

    # 캐시 확인
    cache_key = f"subtitle_{video_id}_{sub_lang}"
    cached_subtitle = subtitle_cache.get(cache_key)
    if cached_subtitle:
        return cached_subtitle

    # 파일 확인
    if os.path.exists(subtitle_path):
        with open(subtitle_path, "r", encoding="utf-8") as file:
            subtitle_content = file.read()
            subtitle_cache.set(cache_key, subtitle_content)
            return subtitle_content

    # 다운로드
    cmd = [
        "yt-dlp",
        "--no-warnings",
        "--write-auto-sub",
        "--sub-lang", sub_lang,
        "--skip-download",
        "--no-playlist",
        "--retries", "3",
        "--socket-timeout", "30",
        "--output", os.path.join(TEMP_FOLDER, "%(id)s.%(ext)s"),
        video_url
    ]

    try:
        execute_command(cmd)
        if os.path.exists(subtitle_path):
            with open(subtitle_path, "r", encoding="utf-8") as file:
                subtitle_content = file.read()
                subtitle_cache.set(cache_key, subtitle_content)
                return subtitle_content
    except Exception as e:
        logger.warning(f"자막 다운로드 실패 ({video_id}): {e}")

    return ""


def clean_subtitles(subtitles):
    # 메모리 효율적인 처리를 위해 한 번에 처리
    cleaned = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d{3}>|"
                     r"\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}|"
                     r"align:[\w\-]+ position:\d+%|"
                     r"<.*?>|\[.*?\]",
                     "", subtitles.replace("\n", " "))

    # 반복 구문 제거 함수
    def remove_repeated_phrases(text):
        words = text.split()
        n = len(words)
        i = 0
        result = []

        while i < n:
            found_repeat = False
            # 최대 검사 길이 제한
            for length in range(1, min(15, n - i)):
                segment = words[i:i + length]
                repetitions = 0
                for j in range(i, min(i + length * 3, n), length):
                    if j + length <= n and words[j:j + length] == segment:
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

    cleaned_subtitles = remove_repeated_phrases(cleaned)
    return re.sub(r"\s{2,}", " ", cleaned_subtitles).strip()


def process_video(video_id, sub_lang):
    global processed_count

    try:
        video = get_video_details(video_id)
        if not video:
            return None

        url = video["url"]
        title = video["title"]
        subtitles = get_subtitles(url, sub_lang)

        if subtitles:
            cleaned = clean_subtitles(subtitles)
            result = {
                "Title": title,
                "Video URL": url,
                "Published At": video.get("published_at"),
                "Subtitles": cleaned
            }

            # 진행 상황 업데이트
            with progress_lock:
                processed_count += 1
                if processed_count % 5 == 0 or processed_count == total_count:
                    logger.info(f"진행 상황: {processed_count}/{total_count} ({processed_count / total_count * 100:.1f}%)")

            return result

    except Exception as e:
        logger.error(f"비디오 처리 중 오류 발생 ({video_id}): {e}")

    # 진행 상황 업데이트 (실패한 경우에도)
    with progress_lock:
        processed_count += 1
        if processed_count % 5 == 0 or processed_count == total_count:
            logger.info(f"진행 상황: {processed_count}/{total_count} ({processed_count / total_count * 100:.1f}%)")

    return None


def process_batch(video_ids, sub_lang, batch_idx, batch_size, output_file):
    batch_results = []

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(process_video, vid, sub_lang) for vid in video_ids]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    batch_results.append(result)
            except Exception as e:
                logger.error(f"Future 실행 중 오류: {e}")

    # 배치 결과 저장
    if batch_results:
        batch_file = f"{output_file}.batch{batch_idx}"
        # 배치 파일 저장 디렉토리 확인
        batch_dir = os.path.dirname(batch_file)
        if not os.path.exists(batch_dir):
            os.makedirs(batch_dir)

        with open(batch_file, "w", encoding="utf-8", newline="") as csvfile:
            fieldnames = ["Published At", "Title", "Video URL", "Subtitles"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(batch_results)

        logger.info(f"배치 {batch_idx} 결과 저장 완료: {len(batch_results)}개 항목")

    return batch_results


def collect_and_save_data(channel_url, sub_lang="ko", max_videos=None, start_date=None, end_date=None):
    global processed_count, total_count

    start_time = time.time()
    logger.info(f"작업 시작: {channel_url}, 언어: {sub_lang}")

    # 비디오 ID 수집
    video_ids = get_video_ids(channel_url, max_videos, start_date, end_date)
    if not video_ids:
        logger.error("영상 ID를 가져오지 못했습니다.")
        return

    # 채널 핸들 추출
    if '@' in channel_url:
        channel_handle = channel_url.split('@')[-1].split('/')[0]
    else:
        channel_handle = channel_url.rstrip('/').split('/')[-1]

    # 채널별 결과 디렉토리 생성
    channel_result_dir = os.path.join(RESULT_FOLDER, channel_handle)
    if not os.path.exists(channel_result_dir):
        os.makedirs(channel_result_dir)

    # 출력 파일 설정
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{channel_handle}_subtitles_{timestamp}.csv"
    output_file = os.path.join(channel_result_dir, output_filename)

    # 전역 카운터 초기화
    total_count = len(video_ids)
    processed_count = 0

    # 배치 처리
    BATCH_SIZE = 500  # 배치 크기 설정
    all_results = []

    for i in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[i:i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        logger.info(f"배치 {batch_idx} 처리 시작: {len(batch)}개 비디오")

        batch_results = process_batch(batch, sub_lang, batch_idx, BATCH_SIZE, output_file)
        all_results.extend(batch_results)

        # 중간 결과 저장
        with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
            fieldnames = ["Published At", "Title", "Video URL", "Subtitles"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)

        logger.info(f"중간 결과 저장 완료: {len(all_results)}개 항목")

    logger.info(f"\n===== 최종 정렬 작업 실행 =====")
    all_results.sort(
        key=lambda x: datetime.fromisoformat(x["Published At"].rstrip("Z"))
    )
    # ──────────────────────────────────────────────────────────

    # ─── 최종 저장 ───────────────────────────────────────────────
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
    # ──────────────────────────────────────────

    # 최종 결과 정리
    elapsed_time = time.time() - start_time
    success_rate = len(all_results) / total_count * 100 if total_count else 0

    logger.info(f"\n===== 작업 완료 =====")
    logger.info(f"채널: {channel_url}")
    logger.info(f"총 영상 수: {total_count}")
    logger.info(f"성공한 영상 수: {len(all_results)} ({success_rate:.1f}%)")
    logger.info(f"처리 시간: {elapsed_time / 60:.1f}분")
    logger.info(f"데이터 저장 위치: {output_file}")



    # 임시 파일 정리
    if input("배치 파일을 정리하시겠습니까? (y/n): ").lower() == 'y':
        # 배치 파일 정리
        for i in range(1, (total_count // BATCH_SIZE) + 2):  # +2는 나머지와 인덱스 1부터 시작을 고려
            batch_file = f"{output_file}.batch{i}"
            if os.path.exists(batch_file):
                os.remove(batch_file)
        logger.info("배치 파일 정리 완료")

def get_valid_input(prompt, validation_func=None, optional=False):
    while True:
        user_input = input(prompt)
        if optional and user_input.strip().lower() in ["skip", "s", ""]:
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
    print("===== YouTube 자막 수집기 =====")
    print(f"현재 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    channel_url = input("YouTube 채널 URL 또는 핸들을 입력하세요: ")
    subtitle_lang = input("자막 언어(ko, en 등)를 입력하세요 [기본값: ko]: ") or "ko"
    max_videos = get_valid_input("최대 영상 수(숫자 또는 'skip'): ", validate_positive_integer, optional=True)
    max_videos = int(max_videos) if max_videos else None

    date_format_msg = "YYYY-MM-DD 형식으로 입력하세요 (또는 'skip'): "
    start_date = get_valid_input(f"시작일 {date_format_msg}", validate_date_format, optional=True)
    end_date = get_valid_input(f"종료일 {date_format_msg}", validate_date_format, optional=True)

    collect_and_save_data(channel_url, subtitle_lang, max_videos, start_date, end_date)