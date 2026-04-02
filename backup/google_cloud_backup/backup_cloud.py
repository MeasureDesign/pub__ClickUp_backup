from __future__ import annotations

import os
import sys
import json
import time
import logging
import traceback
import shutil
import re
from datetime import datetime
from pathlib import Path
import requests
from requests.exceptions import RequestException
from tqdm import tqdm
import shutil
import tempfile
from dataclasses import dataclass
from typing import Any, Optional, Tuple, List
import calendar
from collections import defaultdict
from google.cloud import storage


# API key loaded from environment variables in Google Cloud.
API_TOKEN = os.getenv("API_TOKEN_MAIN")

# Validate that the API token is available before continuing.
if not API_TOKEN:
    print("❌ CHYBA: API token není nastaven!")
    sys.exit(1)

# Prepare the authorization header used for all ClickUp API calls.
HEADERS = {"Authorization": API_TOKEN}

NOW = datetime.now()
SCRIPT_DIR = Path(__file__).resolve().parent

month_prefix = f"{calendar.month_name[NOW.month]}_" if NOW.day == 1 else ""
EXPORT_DIR = SCRIPT_DIR / f"{month_prefix}ClickUp_Backup_Complete_{NOW:%Y%m%d_%H%M%S}"
os.makedirs(EXPORT_DIR, exist_ok=True)

# If empty, all spaces will be downloaded.
TARGET_SPACE_NAME = ""
MAX_RETRIES = 12

# HTTP status codes that should trigger retry behavior.
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

# Progress tracking counters used for summary reporting.
total_tasks_processed = 0
total_attachments_downloaded = 0
total_size_mb = 0

# In-memory logs used to collect successful operations and errors during the run.
success_log = []
error_log = []


# Sanitize file and directory names so they can be safely created on disk.
# This helps avoid filesystem errors caused by invalid characters, trailing dots,
# overly long names, or empty filenames.
def sanitize_filename(filename, max_length=200) -> str:
    # Remove problematic characters.
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)

    # Remove trailing dots and spaces (Windows compatibility).
    sanitized = sanitized.strip('. ')

    # Shorten the filename if it exceeds the allowed maximum length.
    if len(sanitized) > max_length:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:max_length-len(ext)] + ext

    return sanitized if sanitized else "unnamed"


# Configure application logging to both a file inside the export directory
# and standard output. This ensures the backup run can be monitored live
# and also reviewed later if troubleshooting is needed.
def setup_logging() -> None:
    # Logging setup.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(EXPORT_DIR, f"backup_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    print(f"📝 Log file: {log_file}")
    return timestamp


# Check available free disk space at the target path.
# This is mainly used to avoid running out of local storage while downloading
# attachments or writing backup artifacts.
def get_free_space_mb(path: Path) -> float | None:
    try:
        total, used, free = shutil.disk_usage(path)
        return free / (1024 * 1024)
    except Exception as e:
        logging.warning(f"Unable to check free disk space for {path}: {e}")
        return None


# Verify that there is at least a minimum amount of free local disk space.
# This acts as a safety check before large downloads or file writes happen,
# reducing the chance of partial or corrupted backups.
def ensure_min_free_space(path: Path, required_mb: int = 1024) -> bool:
    free_mb = get_free_space_mb(path)

    if free_mb is None:
        # In Cloud Run, treat inability to check as non-fatal unless you want strict behavior.
        return True

    if free_mb < required_mb:
        logging.error(
            f"Low disk space. Available: {free_mb:.1f} MB, required: {required_mb} MB"
        )
        return False

    logging.info(f"Available local space: {free_mb:.1f} MB")
    return True


# Perform a GET request with retry behavior for rate limiting and temporary
# server-side failures. This centralizes resilient API access so the rest
# of the backup logic can rely on a safer request helper.
def safe_get(url, params=None) -> object:
    # Safe GET with retry logic for rate limiting.
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)

            # Retry for rate-limit and server errors.
            if resp.status_code in RETRY_STATUS_CODES:
                retry_in_seconds = (60 - datetime.now().second) + 1
                retry_after = int(resp.headers.get("Retry-After", retry_in_seconds))

                limit_remaining = resp.headers.get("X-RateLimit-Remaining")
                reset_ts = resp.headers.get("X-RateLimit-Reset")
                if resp.status_code == 429 or (limit_remaining is not None and int(limit_remaining) <= 0):
                    if reset_ts:
                        sleep_s = max(1, int(reset_ts) - int(time.time()) + 1)
                    else:
                        sleep_s = int(resp.headers.get("Retry-After", 5))
                    logging.warning(f"Rate limit: sleeping {sleep_s}s")
                    time.sleep(sleep_s)
                    continue
                else:
                    logging.warning(f"Server error {resp.status_code}, retrying in {retry_after}s ({attempt}/{MAX_RETRIES})")

                if attempt < MAX_RETRIES:
                    time.sleep(retry_after)
                    continue

            # 404 is considered acceptable and returned to the caller for handling.
            if resp.status_code == 404:
                logging.debug(f"404 Not Found: {url}")
                return resp

            resp.raise_for_status()
            return resp

        except RequestException as e:
            last_exception = e
            logging.error(f"Network error for {url}: {e} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(min(2 ** attempt, 30))  # Exponential backoff.
                continue

    logging.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
    raise last_exception


# Retrieve all ClickUp spaces across all teams accessible to the API token.
# This is the entry point for discovering what content needs to be backed up
# during the run.
def get_spaces() -> list:
    # Get all spaces with detailed info.
    try:
        print("🔍 Získávám seznam všech teamů a spaces...")
        r = safe_get("https://api.clickup.com/api/v2/team")
        teams = r.json().get("teams", [])
        spaces = []

        print(f"📋 Nalezeno {len(teams)} teamů:")

        for team in teams:
            try:
                team_name = team.get('name', 'Unknown Team')
                team_id = team['id']
                print(f"  🏢 Team: {team_name} (ID: {team_id})")

                r2 = safe_get(f"https://api.clickup.com/api/v2/team/{team_id}/space")
                team_spaces = r2.json().get("spaces", [])

                print(f"    📂 Spaces v tomto teamu ({len(team_spaces)}):")
                for space in team_spaces:
                    space_name = space.get('name', 'Unknown Space')
                    space_id = space.get('id', 'Unknown ID')
                    print(f"      - {space_name} (ID: {space_id})")

                spaces.extend(team_spaces)

            except Exception as e:
                logging.error(f"Error fetching spaces for team {team['id']}: {e}")
                error_log.append({"get_team_spaces": f"{team['id']}: {str(e)}"})

        print(f"\n✅ Celkem nalezeno {len(spaces)} spaces k zálohování")
        return spaces
    except Exception as e:
        logging.error(f"Critical error při načítání spaces: {e}")
        error_log.append({"get_spaces": str(e)})
        return []


# Deduplicate a list of dictionaries by their "id" field.
# This is useful because archived and non-archived API calls may return
# overlapping items that should only be processed once.
def _dedupe_by_id(items):
    d = {}
    for it in items or []:
        iid = it.get("id")
        if iid:
            d[iid] = it
    return list(d.values())


# Retrieve all folders from a space, including archived and non-archived ones.
# This ensures the backup captures both active and archived folder content.
def get_folders(space_id) -> list:
    out = []
    for archived in ("false", "true"):
        r = safe_get(
            f"https://api.clickup.com/api/v2/space/{space_id}/folder",
            params={"archived": archived},
        )
        if r.status_code == 404:
            continue
        out.extend(r.json().get("folders", []))
    return _dedupe_by_id(out)


# Retrieve all lists inside a given folder, including archived and active lists.
# This supports complete folder-level traversal of ClickUp data.
def get_lists_from_folder(folder_id) -> list:
    out = []
    for archived in ("false", "true"):
        r = safe_get(
            f"https://api.clickup.com/api/v2/folder/{folder_id}/list",
            params={"archived": archived},
        )
        if r.status_code == 404:
            continue
        out.extend(r.json().get("lists", []))
    return _dedupe_by_id(out)


# Retrieve all root-level lists that belong directly to a space rather than
# to a folder. This is necessary because ClickUp spaces can contain lists
# both inside folders and directly at the root.
def get_lists_from_space(space_id) -> list:
    out = []
    for archived in ("false", "true"):
        r = safe_get(
            f"https://api.clickup.com/api/v2/space/{space_id}/list",
            params={"archived": archived},
        )
        if r.status_code == 404:
            continue
        out.extend(r.json().get("lists", []))
    return _dedupe_by_id(out)


# Fetch full metadata for a single list.
# This supplements the basic list discovery payload with richer information
# needed for a more complete backup export.
def get_list_details(list_id) -> object:
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/list/{list_id}")
        if r.status_code == 404:
            return None
        return r.json()
    except Exception as e:
        logging.warning(f"Error fetching list details for {list_id}: {e}")
        error_log.append({"get_list_details": f"{list_id}: {e}"})
        return None


# Retrieve all tasks from a list with pagination and include both archived
# and non-archived tasks. The results are deduplicated by task ID so tasks
# are not processed twice.
def get_tasks(list_id) -> list:
    # Get all tasks from a list with pagination info (25 objects per response).
    tasks_by_id = {}

    # Progress bar.
    with tqdm(desc="Loading tasks", unit="tasks", leave=False) as pbar:
        for archived in ("false", "true"):
            page = 0
            while True:
                params = {
                    "page": page,
                    "subtasks": "true",
                    "include_closed": "true",
                    "include_timl": "true",
                    "archived": archived,
                }
                r = safe_get(f"https://api.clickup.com/api/v2/list/{list_id}/task", params=params)
                if r.status_code == 404:
                    break

                batch = r.json().get("tasks", [])
                if not batch:
                    break

                for t in batch:
                    tid = t.get("id")
                    if tid:
                        tasks_by_id[tid] = t
                page += 1
    return list(tasks_by_id.values())


# Retrieve all comments for a task, following ClickUp pagination through
# start_id and start timestamp values. This captures discussion history
# that belongs to the task backup.
def get_comments(task_id) -> list:
    all_comments = []
    params = None

    while True:
        r = safe_get(f"https://api.clickup.com/api/v2/task/{task_id}/comment", params=params)
        if r.status_code == 404:
            break

        batch = r.json().get("comments", [])
        if not batch:
            break

        all_comments.extend(batch)

        last = batch[-1]
        last_id = last.get("id")
        last_date = last.get("date")
        if not last_id or not last_date:
            break

        params = {"start_id": last_id, "start": last_date}

    return all_comments


# Fetch full metadata for a single task.
# This is used to enrich task data beyond the lighter list endpoint response,
# ensuring the backup contains detailed task content and relationships.
def get_task_details(task_id) -> object:
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/task/{task_id}")
        if r.status_code == 404:
            return None
        return r.json()
    except Exception as e:
        logging.warning(f"Error fetching task details for {task_id}: {e}")
        error_log.append({"get_task_details": f"{task_id}: {e}"})
        return None


# Retrieve attachment metadata for a task.
# This is a lightweight helper used when only the attachment information
# is needed without processing the full task object elsewhere.
def get_attachments(task_id) -> list:
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/task/{task_id}")
        if r.status_code == 404:
            return []
        task_data = r.json()
        return task_data.get("attachments", [])
    except Exception as e:
        logging.warning(f"Error fetching attachments metadata for task {task_id}: {e}")
        error_log.append({"get_attachments": f"{task_id}: {e}"})
        return []


# Determine how long the code should wait before retrying a failed request
# for an attachment. It prefers server-provided retry hints and falls back
# to exponential backoff if no explicit guidance exists.
def _retry_sleep_seconds(resp, attempt: int) -> int:
    # Compute how long to sleep on retry-worthy responses.

    # Prefer server guidance.
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1, int(float(ra)))  # Sometimes Retry-After can be "1.0".
        except ValueError:
            pass

    # ClickUp rate limit reset header (when present).
    reset_ts = resp.headers.get("X-RateLimit-Reset")
    if reset_ts:
        try:
            return max(1, int(reset_ts) - int(time.time()) + 1)
        except ValueError:
            pass

    # Fallback: exponential backoff (capped).
    return min(2 ** min(attempt, 6), 60)


# Download a single attachment file to disk with retry support, temporary
# file handling, disk-space checks, and progress reporting. This function
# is designed to make file downloads safer and less likely to leave corrupt
# partial files behind.
def download_attachment(url, dest_path, desc="Downloading"):
    # Returns a tuple containing success state and downloaded size in MB.
    global total_attachments_downloaded, total_size_mb

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    tmp_path = dest_path + ".part"
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Many ClickUp attachment URLs are CDN or pre-signed.
            # Keeping HEADERS usually doesn't hurt, but if you see weird 403s from S3,
            # try headers=None here.
            r = requests.get(url, headers=HEADERS, stream=True, timeout=(10, 60))
            status = r.status_code

            # Handle retryable HTTP statuses.
            if status in RETRY_STATUS_CODES:
                sleep_s = _retry_sleep_seconds(r, attempt)
                if status == 429:
                    logging.warning(f"Attachment rate-limited, sleeping {sleep_s}s ({attempt}/{MAX_RETRIES})")
                else:
                    logging.warning(f"Attachment server error {status}, sleeping {sleep_s}s ({attempt}/{MAX_RETRIES})")
                r.close()
                time.sleep(sleep_s)
                continue

            # Non-retryable cases.
            if status in (401, 403):
                logging.warning(f"Attachment auth error {status}: {url} (skipping)")
                r.close()
                return False, 0.0

            if status == 404:
                logging.warning(f"Attachment 404: {url}")
                r.close()
                return False, 0.0

            if status == 410:
                logging.warning(f"Attachment 410 Gone: {url}")
                r.close()
                return False, 0.0

            r.raise_for_status()

            total_size = int(r.headers.get("content-length", 0))

            # Disk space check before writing the file.
            if not ensure_min_free_space(EXPORT_DIR, required_mb=1024):
                err = f"not enough space."
                logging.error(f"Error downloading attachment {url} to {dest_path}: {err}")
                error_log.append({"download_attachment": f"{url}: {err}"})
                r.close()
                return False, 0.0

            # Stream to a temporary file first.
            with open(tmp_path, "wb") as f, tqdm(
                desc=desc,
                total=total_size if total_size > 0 else None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                leave=False
            ) as pbar:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))

            r.close()

            # Atomically move the finished file into place.
            os.replace(tmp_path, dest_path)

            size_mb = os.path.getsize(dest_path) / (1024 * 1024)
            total_size_mb += size_mb
            total_attachments_downloaded += 1

            logging.debug(f"Successfully downloaded: {dest_path} ({size_mb:.2f} MB)")
            return True, size_mb

        except RequestException as e:
            last_exception = e
            logging.warning(f"Network error downloading {url}: {e} ({attempt}/{MAX_RETRIES})")

            # Remove partial temp file if it exists.
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

            if attempt < MAX_RETRIES:
                time.sleep(min(2 ** min(attempt, 6), 60))
                continue

        except Exception as e:
            last_exception = e
            logging.error(f"Error downloading attachment {url} to {dest_path}: {e}")
            error_log.append({"download_attachment": f"{url}: {str(e)}"})

            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

            if attempt < MAX_RETRIES:
                time.sleep(min(2 ** min(attempt, 6), 60))
                continue

    logging.error(f"Failed to download {url} after {MAX_RETRIES} attempts: {last_exception}")
    return False, 0.0


# Safely write JSON data to disk using a temporary backup file and atomic rename.
# This reduces the chance of leaving a half-written JSON file if the process
# fails during the write operation.
def safe_file_write(file_path, data) -> bool:
    # Write JSON file.
    backup_path = f"{file_path}.backup"
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(backup_path, file_path)
        return True
    except Exception as e:
        logging.error(f"Error writing to {file_path}: {e}")
        if os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except:
                pass
        return False


# Process a single ClickUp list by fetching its metadata, tasks, comments,
# and attachments, then building the backup structure for that list.
# This function performs the main per-list export work.
def process_list(lst, space_dir) -> dict:
    global total_tasks_processed

    list_id = lst.get("id")
    list_name = sanitize_filename(lst.get("name", "Unknown"))

    logging.info(f"    » Processing list '{list_name}' (ID: {list_id})")

    list_details = get_list_details(list_id)

    tasks = get_tasks(list_id)
    logging.info(f"      Found {len(tasks)} tasks")

    list_data = {
        "name": list_name,
        "id": list_id,
        "tasks": [],
        "processed_at": datetime.now().isoformat(),
        "task_count": len(tasks),
        "metadata": {
            "content": list_details.get("content") if list_details else None,
            "order_index": list_details.get("orderindex") if list_details else None,
            "status": list_details.get("status") if list_details else None,
            "priority": list_details.get("priority") if list_details else None,
            "assignee": list_details.get("assignee") if list_details else None,
            "due_date": list_details.get("due_date") if list_details else None,
            "start_date": list_details.get("start_date") if list_details else None,
            "folder": list_details.get("folder") if list_details else None,
            "space": list_details.get("space") if list_details else None,
            "archived": list_details.get("archived") if list_details else None,
            "override_statuses": list_details.get("override_statuses") if list_details else None,
            "statuses": list_details.get("statuses") if list_details else [],
        }
    }
    total_attachments_size = 0

    with tqdm(tasks, desc=f"Processing {list_name[:30]}", unit="tasks", leave=False) as pbar:
        for task in pbar:
            try:
                tid = task.get("id")
                task_name = task.get("name", "Unnamed Task")
                pbar.set_postfix_str(f"Current: {task_name[:20]}...")

                total_tasks_processed += 1

                task_full = get_task_details(tid)
                if not task_full:
                    task_full = task

                comments = get_comments(tid)

                attachments_meta = task_full.get("attachments", [])
                att_data = []

                if attachments_meta:
                    att_dir = os.path.join(space_dir, "attachments", str(tid))
                    for att in attachments_meta:
                        url = att.get("url")
                        if not url:
                            continue

                        att_id = att.get("id", "unknown")
                        original_name = att.get("title") or str(att_id)

                        fname = f"{att_id}__{sanitize_filename(original_name)}"
                        dest = os.path.join(att_dir, fname)

                        if os.path.exists(dest):
                            name, ext = os.path.splitext(fname)
                            fname = f"{name}_{int(time.time())}{ext}"
                            dest = os.path.join(att_dir, fname)

                        success, size_mb = download_attachment(url, dest, f"DL: {fname[:15]}")
                        rel_path = os.path.relpath(dest, space_dir)
                        att_data.append({
                            "id": att.get("id"),
                            "title": att.get("title"),
                            "url": att.get("url"),
                            "local_file": rel_path if success else None,
                            "size": att.get("size"),
                            "extension": att.get("extension"),
                            "date": att.get("date"),
                            "user": att.get("user"),
                            "downloaded": success
                        })

                        if success:
                            total_attachments_size += size_mb

                task_backup = {
                    "id": tid,
                    "name": task_name,
                    "text_content": task_full.get("text_content"),
                    "description": task_full.get("description"),
                    "status": task_full.get("status"),
                    "orderindex": task_full.get("orderindex"),
                    "date_created": task_full.get("date_created"),
                    "date_updated": task_full.get("date_updated"),
                    "date_closed": task_full.get("date_closed"),
                    "date_done": task_full.get("date_done"),
                    "archived": task_full.get("archived"),
                    "creator": task_full.get("creator"),
                    "assignees": task_full.get("assignees", []),
                    "watchers": task_full.get("watchers", []),
                    "checklists": task_full.get("checklists", []),
                    "tags": task_full.get("tags", []),
                    "parent": task_full.get("parent"),
                    "priority": task_full.get("priority"),
                    "due_date": task_full.get("due_date"),
                    "start_date": task_full.get("start_date"),
                    "points": task_full.get("points"),
                    "time_estimate": task_full.get("time_estimate"),
                    "time_spent": task_full.get("time_spent"),
                    "custom_fields": task_full.get("custom_fields", []),
                    "dependencies": task_full.get("dependencies", []),
                    "linked_tasks": task_full.get("linked_tasks", []),
                    "team_id": task_full.get("team_id"),
                    "url": task_full.get("url"),
                    "sharing": task_full.get("sharing"),
                    "permission_level": task_full.get("permission_level"),
                    "list": task_full.get("list"),
                    "project": task_full.get("project"),
                    "folder": task_full.get("folder"),
                    "space": task_full.get("space"),
                    "comments": comments,
                    "attachments": att_data
                }

                list_data["tasks"].append(task_backup)

            except Exception as ex:
                logging.error(f"Error processing task {tid}: {ex}")
                error_log.append({"process_task": f"{tid}: {traceback.format_exc()}"})

    logging.info(f"      ✅ List '{list_name}' completed: {len(tasks)} tasks, {total_attachments_size:.2f} MB attachments")
    return list_data


# Fetch full metadata for a single space.
# This is used to enrich the exported space structure with additional settings
# and configuration information.
def get_space_details(space_id) -> object | None:
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/space/{space_id}")
        if r.status_code == 404:
            return None
        return r.json()
    except Exception as e:
        logging.warning(f"Error fetching space details for {space_id}: {e}")
        error_log.append({"get_space_details": f"{space_id}: {e}"})
        return None


# Fetch full metadata for a single folder.
# This supplements folder discovery with detailed properties needed in the backup.
def get_folder_details(folder_id) -> object | None:
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/folder/{folder_id}")
        if r.status_code == 404:
            return None
        return r.json()
    except Exception as e:
        logging.warning(f"Error fetching folder details for {folder_id}: {e}")
        error_log.append({"get_folder_details": f"{folder_id}: {e}"})
        return None


# Back up a single space by exporting its folders, lists, tasks, comments,
# attachments, and metadata into a dedicated directory and JSON file.
# This is the main unit of work for each ClickUp space.
def backup_space(space) -> None:
    # Main function for backing up spaces.
    space_id = space.get("id")
    space_name = sanitize_filename(space.get("name", "Unknown_Space"))

    if TARGET_SPACE_NAME and space_name != TARGET_SPACE_NAME:
        return

    print(f"\n🚀 Starting backup of space: {space_name} (ID: {space_id})")
    logging.info(f"🚀 Starting backup of space: {space_name} (ID: {space_id})")

    space_details = get_space_details(space_id)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    space_dir = os.path.join(EXPORT_DIR, f"{space_name}_{space_id}_{timestamp}")
    os.makedirs(space_dir, exist_ok=True)

    space_data = {
        "name": space_name,
        "id": space_id,
        "folders": [],
        "lists": [],
        "backup_timestamp": timestamp,
        "backup_date": datetime.now().isoformat(),
        "metadata": {
            "private": space_details.get("private") if space_details else None,
            "statuses": space_details.get("statuses") if space_details else [],
            "multiple_assignees": space_details.get("multiple_assignees") if space_details else None,
            "features": space_details.get("features") if space_details else {},
            "archived": space_details.get("archived") if space_details else None,
        },
        "backup_stats": {
            "total_folders": 0,
            "total_lists": 0,
            "total_tasks": 0,
            "total_attachments": 0
        }
    }

    folders = get_folders(space_id)
    print(f"  📂 Found {len(folders)} folders")
    logging.info(f"  Found {len(folders)} folders")

    for folder in tqdm(folders, desc="Processing folders", leave=False):
        folder_name = folder.get("name", "Unknown_Folder")
        folder_id = folder.get("id")
        print(f"  📂 Processing folder: {folder_name}")
        logging.info(f"  📂 Processing folder: {folder_name}")

        folder_details = get_folder_details(folder_id)

        folder_data = {
            "name": folder_name,
            "id": folder_id,
            "lists": [],
            "metadata": {
                "orderindex": folder_details.get("orderindex") if folder_details else None,
                "override_statuses": folder_details.get("override_statuses") if folder_details else None,
                "hidden": folder_details.get("hidden") if folder_details else None,
                "space": folder_details.get("space") if folder_details else None,
                "task_count": folder_details.get("task_count") if folder_details else None,
                "archived": folder_details.get("archived") if folder_details else None,
                "statuses": folder_details.get("statuses") if folder_details else [],
            }
        }

        folder_lists = get_lists_from_folder(folder_id)
        for lst in folder_lists:
            list_data = process_list(lst, space_dir)
            folder_data["lists"].append(list_data)
            space_data["backup_stats"]["total_tasks"] += list_data["task_count"]

        space_data["folders"].append(folder_data)
        space_data["backup_stats"]["total_folders"] += 1
        space_data["backup_stats"]["total_lists"] += len(folder_lists)

    root_lists = get_lists_from_space(space_id)
    print(f"  📋 Found {len(root_lists)} root lists")
    logging.info(f"  Found {len(root_lists)} root lists")

    for lst in tqdm(root_lists, desc="Processing root lists", leave=False):
        list_data = process_list(lst, space_dir)
        space_data["lists"].append(list_data)
        space_data["backup_stats"]["total_tasks"] += list_data["task_count"]

    space_data["backup_stats"]["total_lists"] += len(root_lists)
    space_data["backup_stats"]["total_attachments"] = total_attachments_downloaded

    json_path = os.path.join(space_dir, f"backup_{space_name}_{space_id}_{timestamp}.json")
    if safe_file_write(json_path, space_data):
        success_log.append(f"Backup completed for space: {space_name}")
        print(f"✅ Space '{space_name}' backup completed successfully")
        logging.info(f"✅ Space '{space_name}' backup completed successfully")
    else:
        error_log.append(f"Failed to write main backup file for space: {space_name}")
        print(f"❌ Failed to write main backup file for space: {space_name}")
        logging.error(f"❌ Failed to write main backup file for space: {space_name}")


# Generate a backup summary containing totals, success entries, and errors,
# then write it to disk. This gives a compact overview of the whole run.
def generate_summary_report() -> dict:
    summary = {
        "backup_summary": {
            "timestamp": datetime.now().isoformat(),
            "total_spaces_processed": len(success_log),
            "total_tasks_processed": total_tasks_processed,
            "total_attachments_downloaded": total_attachments_downloaded,
            "total_size_mb": round(total_size_mb, 2),
            "total_errors": len(error_log)
        },
        "success_log": success_log,
        "error_log": error_log
    }

    summary_path = os.path.join(EXPORT_DIR, "backup_summary.json")
    safe_file_write(summary_path, summary)
    return summary


# Write plain text atomically by using a temporary file and then replacing
# the target. This helps prevent half-written files if the process crashes.
def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding=encoding) as tf:
        tf.write(text)
        tmp_name = tf.name
    os.replace(tmp_name, path)  # Atomic rename on the same filesystem.


# Serialize data as JSON and write it atomically.
# This is used for important backup artifacts such as manifests and logs,
# where partial writes would make recovery or validation harder.
def atomic_write_json(path: Path, data: Any, encoding: str = "utf-8") -> None:
    text = json.dumps(data, indent=2, ensure_ascii=False)
    atomic_write_text(path, text, encoding=encoding)


# Represent a validation problem found in the exported backup directory.
# The severity field distinguishes critical issues from noncritical ones
# so final run status can be decided more accurately.
@dataclass
class ValidationIssue:
    severity: str       # "critical" or "noncritical"
    where: str          # e.g. "validate"
    file: str
    error: str


# Check whether a file exists, is a regular file, can be opened, and is not empty.
# This is a generic integrity helper reused by the validation layer.
def _is_readable_file(p: Path) -> Tuple[bool, str]:
    if not p.exists():
        return False, "missing"
    if not p.is_file():
        return False, "not a file"
    try:
        # Basic permission check; still try open because ACLs can lie.
        if not os.access(p, os.R_OK):
            return False, "not readable (os.access)"
        with p.open("rb") as f:
            chunk = f.read(64)
        if len(chunk) == 0:
            return False, "empty file"
        return True, ""
    except Exception as e:
        return False, f"open/read failed: {e}"


# Validate that a JSON file is present and structurally valid.
# Smaller files are fully parsed, while very large files get a cheaper
# structural check to avoid excessive memory usage.
def _validate_json_file(p: Path, max_parse_bytes: int = 50_000_000) -> Tuple[bool, str]:
    # Returns a tuple containing validation state and an error message.
    # If the file is huge, do a cheap structural check to avoid loading
    # a multi-GB JSON file into memory.
    ok, err = _is_readable_file(p)
    if not ok:
        return False, err

    try:
        size = p.stat().st_size
        if size <= max_parse_bytes:
            with p.open("r", encoding="utf-8") as f:
                json.load(f)
            return True, ""

        # Cheap check for very large JSON: starts and ends with a JSON container.
        with p.open("rb") as f:
            head = f.read(256).lstrip()
            f.seek(max(0, size - 256))
            tail = f.read(256).rstrip()

        if not head:
            return False, "empty file"
        if head[:1] not in (b"{", b"["):
            return False, "does not start with '{' or '['"
        if tail[-1:] not in (b"}", b"]"):
            return False, "does not end with '}' or ']'"
        return True, ""
    except Exception as e:
        return False, f"json validation failed: {e}"


# Validate the export directory after the backup finishes.
# This makes sure the expected key artifacts exist and are readable, so the
# run can be marked failed if the output is incomplete or corrupted.
def validate_export_dir(export_dir: Path, *, skip_names: set[str] | None = None) -> list[ValidationIssue]:
    skip_names = skip_names or set()

    issues: list[ValidationIssue] = []

    required_text = [export_dir / "log_success.txt"]
    required_json = [
        export_dir / "backup_summary.json",
    ]

    for p in required_text:
        if p.name in skip_names:
            continue
        ok, err = _is_readable_file(p)
        if not ok:
            issues.append(ValidationIssue("critical", "validate", str(p), err))

    for p in required_json:
        if p.name in skip_names:
            continue
        ok, err = _validate_json_file(p)
        if not ok:
            issues.append(ValidationIssue("critical", "validate", str(p), err))

    return issues


# Create a ZIP archive from the given source directory.
# This is used to package the final backup into a single uploadable artifact.
def make_zip_from_dir(source_dir: str) -> Path:
    source = Path(source_dir)
    zip_base = source.parent / source.name
    zip_path = shutil.make_archive(str(zip_base), "zip", root_dir=str(source))
    return Path(zip_path)


# Upload a local file to Google Cloud Storage under the specified remote path.
# This is used to push the final backup ZIP and manifest into the bucket.
def upload_file(bucket_name: str, local_path: str, remote_path: str) -> None:
    try:
        client = storage.Client(project=os.getenv("GCLOUD_PROJECT"))
        bucket = client.bucket(os.getenv("BUCKET_NAME"))
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_path)
        logging.info(f"Successfully uploaded file to GCLOUD BUCKET: {bucket_name}")
    except Exception as e:
        logging.error(f"Error uploading file to gcloud bucket: {bucket_name}, error: {e}")


# Describe one uploaded backup run in Google Cloud Storage.
# This makes retention handling simpler by keeping parsed metadata together
# in a structured immutable record.
@dataclass(frozen=True)
class GCSRunInfo:
    prefix: str              # e.g. "daily/ClickUp_Backup_Complete_20260306_102054/"
    status: str
    sort_ts: float
    monthly_key: str | None  # e.g. "2026-03" for monthly runs
    manifest_blob: str       # Full blob name.
    kind: str                # "daily" or "monthly"


# Convert a run timestamp into a YYYY-MM key for monthly grouping.
# This supports retention rules that keep one representative run per month.
def _derive_monthly_key(sort_ts: float) -> str:
    return datetime.fromtimestamp(sort_ts).strftime("%Y-%m")


# Parse manifest.json bytes downloaded from GCS and extract the backup status
# and sort timestamp used by retention logic.
def _parse_manifest_bytes(data: bytes) -> tuple[str, float]:
    status = "UNKNOWN"
    sort_ts = datetime.now().timestamp()

    m = json.loads(data.decode("utf-8"))
    status = str(m.get("run_status", "UNKNOWN"))

    ended_iso = m.get("ended_at_iso")
    if ended_iso:
        sort_ts = datetime.fromisoformat(ended_iso).timestamp()
    else:
        ended_ts = m.get("ended_at_ts")
        if isinstance(ended_ts, (int, float)):
            sort_ts = float(ended_ts)

    return status, sort_ts


# List all manifest.json objects stored under the daily/ and monthly/
# prefixes in the GCS bucket. Manifests are used as the source of truth
# for retention decisions.
def _list_manifest_blobs(bucket: storage.Bucket) -> list[str]:
    manifest_blobs: list[str] = []

    for root in ("daily/", "monthly/"):
        for blob in bucket.list_blobs(prefix=root):
            if blob.name.endswith("/manifest.json"):
                manifest_blobs.append(blob.name)

    return manifest_blobs


# Derive the backup prefix directory from a manifest object path.
# This allows retention logic to delete the whole backup folder instead
# of only the manifest file.
def _prefix_from_manifest_name(manifest_name: str) -> str:
    return manifest_name.removesuffix("manifest.json")


# Determine whether a backup prefix belongs to daily or monthly storage.
# This is used to apply the appropriate retention rule set.
def _kind_from_prefix(prefix: str) -> str:
    if prefix.startswith("daily/"):
        return "daily"
    if prefix.startswith("monthly/"):
        return "monthly"
    return "unknown"


# Load and parse all backup runs from GCS into structured records.
# This builds the in-memory view used later by the retention algorithm.
def _load_runs_from_gcs(bucket: storage.Bucket) -> list[GCSRunInfo]:
    runs: list[GCSRunInfo] = []

    for manifest_name in _list_manifest_blobs(bucket):
        blob = bucket.blob(manifest_name)

        status = "UNKNOWN"
        sort_ts = datetime.now().timestamp()

        try:
            data = blob.download_as_bytes()
            status, sort_ts = _parse_manifest_bytes(data)
        except Exception as e:
            logging.warning(f"Retention: failed to parse manifest {manifest_name}: {e}")

        prefix = _prefix_from_manifest_name(manifest_name)
        kind = _kind_from_prefix(prefix)
        monthly_key = _derive_monthly_key(sort_ts) if kind == "monthly" else None

        runs.append(
            GCSRunInfo(
                prefix=prefix,
                status=status,
                sort_ts=sort_ts,
                monthly_key=monthly_key,
                manifest_blob=manifest_name,
                kind=kind,
            )
        )

    runs.sort(key=lambda r: r.sort_ts, reverse=True)
    return runs


# Delete all objects under a given GCS prefix, or log what would be deleted
# if dry-run mode is enabled. This is the low-level delete helper for retention.
def _delete_prefix(bucket: storage.Bucket, prefix: str, *, dry_run: bool = False) -> None:
    blobs = list(bucket.list_blobs(prefix=prefix))

    if dry_run:
        logging.info(f"[DRY RUN] Would delete {len(blobs)} blob(s) under {prefix}")
        return

    for blob in blobs:
        blob.delete()

    logging.info(f"Retention: deleted {len(blobs)} blob(s) under {prefix}")


# Apply retention rules to backups stored in GCS.
# It keeps a configured number of recent successful and non-successful daily
# runs, and for monthly backups it keeps the newest successful run per month
# or the newest run overall if no success exists.
def enforce_retention_gcs(
    bucket_name: str,
    project_id: str,
    current_run_prefix: str,
    keep_success: int = 3,
    keep_non_success: int = 2,
    dry_run: bool = False,
) -> None:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    runs = _load_runs_from_gcs(bucket)

    keep_set: set[str] = {current_run_prefix}

    # Daily retention.
    daily_runs = [r for r in runs if r.kind == "daily"]
    daily_successes = [r for r in daily_runs if r.status == "SUCCESS"]
    daily_nons = [r for r in daily_runs if r.status != "SUCCESS"]

    keep_set |= {r.prefix for r in daily_successes[:keep_success]}
    keep_set |= {r.prefix for r in daily_nons[:keep_non_success]}

    # Monthly retention.
    # Keep newest SUCCESS per month; otherwise keep the newest run for that month.
    monthly_groups: dict[str, list[GCSRunInfo]] = defaultdict(list)

    for r in runs:
        if r.kind == "monthly" and r.monthly_key is not None:
            monthly_groups[r.monthly_key].append(r)

    for month_key, group in monthly_groups.items():
        group.sort(key=lambda r: r.sort_ts, reverse=True)
        keep_candidate = next((r for r in group if r.status == "SUCCESS"), group[0])
        keep_set.add(keep_candidate.prefix)

    # Deletion pass.
    for r in runs:
        if r.prefix in keep_set:
            continue

        try:
            logging.info(
                f"Retention: deleting old backup prefix {r.prefix} "
                f"(status={r.status}, kind={r.kind}, monthly_key={r.monthly_key})"
            )
            _delete_prefix(bucket, r.prefix, dry_run=dry_run)
        except Exception as e:
            logging.error(f"Retention: failed to delete prefix {r.prefix}: {e}")


# Run the full backup process from start to finish.
# This orchestrates setup, space discovery, export, validation, manifest
# creation, upload, and retention handling.
def run_backup():
    # Main function.
    setup_logging()

    print("🚀 Starting COMPLETE ClickUp backup process")
    print(f"📁 Export directory: {os.path.abspath(EXPORT_DIR)}")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    logging.info("🚀 Starting ClickUp backup process")
    logging.info(f"📁 Export directory: {os.path.abspath(EXPORT_DIR)}")

    # Disk space check.
    if not ensure_min_free_space(EXPORT_DIR, required_mb=1024):
        return 1

    start_time = time.time()

    # Get the list of spaces.
    spaces = get_spaces()
    if not spaces:
        print("❌ No spaces found or failed to fetch spaces!")
        logging.error("❌ No spaces found or failed to fetch spaces!")
        return

    print(f"\n📊 BACKUP PLAN:")
    print(f"   📂 Spaces to backup: {len(spaces)}")
    print(f"   📁 Output directory: {EXPORT_DIR}")
    print(f"   ⏰ Estimated time: {len(spaces) * 2}-{len(spaces) * 5} minutes")
    print(f"   📄 Note: Docs will be skipped (not available via API)")
    print(f"   💾 Disk space will be checked automatically")
    print(f"\n⏯️  Starting backup process...")
    time.sleep(1)

    # Back up each space.
    for i, space in enumerate(spaces, 1):
        space_name = space.get("name", "Unknown")

        print(f"\n[{i}/{len(spaces)}] 🎯 Processing space: {space_name}")
        logging.info(f"[{i}/{len(spaces)}] Processing space: {space_name}")
        try:
            backup_space(space)
        except Exception as e:
            print(f"❌ Critical error backing up space {space_name}: {e}")
            logging.error(f"Critical error backing up space {space_name}: {e}")
            error_log.append({
                "severity": "critical",
                "scope": "space",
                "space_name": space_name,
                "space_id": space.get("id"),
                "where": "backup_space",
                "error": str(e),
                "trace": traceback.format_exc(),
            })

    end_time = time.time()
    duration = end_time - start_time

    # Make summary report.
    summary = generate_summary_report()

    print(f"\n🎉 BACKUP COMPLETED!")
    print(f"⏱️  Duration: {duration/60:.1f} minutes")
    print(f"📊 Statistics:")
    print(f"   ✅ Spaces: {len(success_log)}")
    print(f"   📋 Tasks: {total_tasks_processed}")
    print(f"   📎 Attachments: {total_attachments_downloaded}")
    print(f"   💾 Total size: {total_size_mb:.1f} MB")
    print(f"   ❌ Errors: {len(error_log)}")
    print(f"📁 Data saved to: {os.path.abspath(EXPORT_DIR)}")
    print(f"📄 For docs: Use manual export from ClickUp interface")

    logging.info("✅ Backup process completed!")
    logging.info(f"📊 Duration: {duration/60:.1f} minutes")
    logging.info(f"📊 Stats: {len(success_log)} spaces, {total_tasks_processed} tasks, {total_attachments_downloaded} attachments")

    if error_log:
        print(f"⚠️  There were {len(error_log)} errors. Check {log_errors_path} for details.")
        logging.warning(f"⚠️  There were {len(error_log)} errors. Check {log_errors_path} for details.")

    print(f"\n📋 Summary report: backup_summary.json")

    run_status: str = ""

    # Finalization stage:
    # decide final status, validate output, write log artifacts, and apply retention.

    export_dir = Path(EXPORT_DIR)
    log_success_path = export_dir / "log_success.txt"
    log_errors_path = export_dir / "log_errors.json"

    spaces_planned = len(spaces)

    # This is only correct if success_log contains one entry per successful space.
    spaces_ok = len(success_log)
    spaces_failed = spaces_planned - spaces_ok

    # Count total, critical, and noncritical errors for reporting and status decisions.
    def compute_counts() -> tuple[int, int, int]:
        errors_total = len(error_log)
        critical = sum(1 for e in error_log if e.get("severity") == "critical")
        noncritical = errors_total - critical
        return errors_total, critical, noncritical

    # Decide the overall run status based on completed spaces and critical failures.
    def decide_status(spaces_ok: int, spaces_planned: int, critical_errors: int) -> str:
        if spaces_ok == 0 or critical_errors > 0:
            return "FAILED"
        if spaces_ok < spaces_planned:
            return "PARTIAL"
        return "SUCCESS"

    # Initial status calculation.
    errors_total, critical_errors, noncritical_errors = compute_counts()
    run_status = decide_status(spaces_ok, spaces_planned, critical_errors)

    atomic_write_text(log_success_path, "\n".join(success_log) + "\n")
    atomic_write_json(log_errors_path, error_log)

    # Validate export directory and append any validation issues.
    validation_issues = validate_export_dir(export_dir)
    for issue in validation_issues:
        error_log.append({
            "severity": issue.severity,
            "scope": "global",
            "where": issue.where,
            "file": issue.file,
            "error": issue.error,
        })

    atomic_write_json(log_errors_path, error_log)

    # Recompute counts after validation issues were appended.
    errors_total, critical_errors, noncritical_errors = compute_counts()
    run_status = decide_status(spaces_ok, spaces_planned, critical_errors)

    # If validation found critical issues, force FAILED as a final safety measure.
    if any(i.severity == "critical" for i in validation_issues):
        run_status = "FAILED"

    # Write manifest and final status marker file.
    started_at_iso = datetime.fromtimestamp(start_time).isoformat(timespec="seconds")
    ended_at_iso = datetime.fromtimestamp(end_time).isoformat(timespec="seconds")

    manifest = {
        "started_at_iso": started_at_iso,
        "ended_at_iso": ended_at_iso,
        "started_at_ts": start_time,
        "ended_at_ts": end_time,
        "duration_seconds": duration,
        "run_status": run_status,

        "spaces_planned": spaces_planned,
        "spaces_ok": spaces_ok,
        "spaces_failed": spaces_failed,

        "tasks": total_tasks_processed,
        "attachments_downloaded": total_attachments_downloaded,
        "total_size_mb": total_size_mb,

        "errors_total": errors_total,
        "critical_errors": critical_errors,
        "noncritical_errors": noncritical_errors,

        "path_to_log_errors": str(log_errors_path.resolve()),
    }

    atomic_write_json(export_dir / "manifest.json", manifest)
    atomic_write_text(export_dir / run_status, ended_at_iso + "\n")

    logging.info(f"Run status: {run_status}")

    # Zip the whole backup directory for upload.
    zip_path: Path = make_zip_from_dir(export_dir)
    if run_status == "SUCCESS":
        # On successful runs, upload the ZIP and manifest to the GCS bucket,
        # then apply GCS retention rules.

        bucket_name: str = os.getenv("BUCKET_NAME")
        local_path_upload: str = zip_path
        remote_path_upload_prefix: str = f"monthly" if NOW.day == 1 else "daily"
        remote_path_upload: str = f"{remote_path_upload_prefix}/ClickUp_Backup_Complete_{NOW:%Y%m%d_%H%M%S}/backup.zip"

        manifest_path: str = f"{export_dir}/manifest.json"
        upload_file(bucket_name, manifest_path, f"{remote_path_upload_prefix}/ClickUp_Backup_Complete_{NOW:%Y%m%d_%H%M%S}/manifest.json")
        upload_file(bucket_name, local_path_upload, remote_path_upload)

        current_run_prefix: str = remote_path_upload_prefix
        enforce_retention_gcs(
            bucket_name=bucket_name,
            project_id=os.getenv("GCLOUD_PROJECT"),
            current_run_prefix=current_run_prefix,
            keep_success=3,
            keep_non_success=2,
            dry_run=False,
        )
    else:
        logging.info(f"Retention: skipped (run_status={run_status})")


# Program entry wrapper.
# This ensures the script exits with an appropriate return code and that
# unexpected top-level failures are recorded in the log.
def main():
    try:
        run_backup()
        return (0)
    except Exception:
        logging.exception("Critical error")
        return (1)


# Standard Python entry point.
# Running the file directly executes main() and exits with its return code.
if __name__ == "__main__":
    raise SystemExit(main())