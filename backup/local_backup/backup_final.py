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
from requests.exceptions import RequestException  # , HTTPError
from tqdm import tqdm  # loading bar
from dotenv import load_dotenv  # for reading .env files (for token)
import shutil
import tempfile
from dataclasses import dataclass
from typing import Any, Optional, Tuple, List

# Load API token from the .env file.
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN_MAIN")

# Validate that the API token is available before continuing.
if not API_TOKEN:
    print("❌ ERROR: API token is not set!")
    sys.exit(1)

# Prepare the authorization header used for all ClickUp API calls.
HEADERS = {"Authorization": API_TOKEN}

# Output directory - it will be created in the same directory as the script.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(SCRIPT_DIR, f"ClickUp_Backup_Complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
os.makedirs(EXPORT_DIR, exist_ok=True)

# Download ALL spaces.
TARGET_SPACE_NAME = ""  # Empty = all spaces

# Retry configuration for rate limits and server errors.
# It is intentionally set relatively high because ClickUp rate limiting
# can be strict (100 req/min). If there are many spaces with a lot of data,
# the process will take time.
MAX_RETRIES = 10  # 200 would effectively mean an hour
DEFAULT_RETRY_AFTER = 5  # in seconds
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]  # status codes to retry

# Progress tracking.
total_tasks_processed = 0
total_attachments_downloaded = 0
total_size_mb = 0

# Success and error logs.
success_log = []
error_log = []


# Sanitize a file name so it can be safely used on the local filesystem.
# This removes problematic characters, trims trailing dots/spaces,
# and enforces a maximum length to avoid OS-specific path issues.
def sanitize_filename(filename, max_length=200) -> str:
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
    sanitized = sanitized.strip('. ')
    if len(sanitized) > max_length:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:max_length-len(ext)] + ext
    return sanitized if sanitized else "unnamed"


# Configure logging so output is written both to a log file and the console.
# The function also returns the timestamp used in the log file name.
def setup_logging() -> None:
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


# Check currently available free disk space in the provided directory.
# This is used mainly before downloading attachments to reduce the risk
# of failing mid-download due to insufficient space.
def check_current_disk_space(required_mb, dir_path) -> int:
    total, used, free = shutil.disk_usage(dir_path)
    free_mb = free / (1024 * 1024)
    return (free_mb)


# Check whether the export directory has enough free space before the backup starts.
# If the available space is below the required threshold, the script exits early.
def check_disk_space(required_mb=1024) -> None:
    try:
        total, used, free = shutil.disk_usage(EXPORT_DIR)
        free_mb = free / (1024 * 1024)

        if free_mb < required_mb:
            print(f"⚠️  WARNING: Low disk space!")
            print(f"   Available: {free_mb:.1f} MB")
            print(f"   Recommended: {required_mb} MB")
            logging.warning(f"Low disk space: {free_mb:.1f} MB available")
            sys.exit(1)
        else:
            print(f"💾 Available space: {free_mb:.1f} MB")

    except Exception as e:
        logging.warning(f"Unable to check disk space: {e}")


# Perform a GET request with retry logic for rate limits and transient server errors.
# This wrapper centralizes API request handling and returns the response object,
# including 404 responses when that is considered acceptable by callers.
def safe_get(url, params=None) -> object:
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)

            # Retry for rate limits and server errors.
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

            # 404 is acceptable here - return the response.
            if resp.status_code == 404:
                logging.debug(f"404 Not Found: {url}")
                return resp

            resp.raise_for_status()
            return resp

        except RequestException as e:
            last_exception = e
            logging.error(f"Network error for {url}: {e} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(min(2 ** attempt, 30))  # exponential backoff
                continue

    logging.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
    raise last_exception


# Fetch all teams available to the authenticated user and then fetch all spaces
# from those teams. The function returns a flat list of spaces across all teams.
def get_spaces() -> list:
    try:
        print("🔍 Fetching all teams and spaces...")
        r = safe_get("https://api.clickup.com/api/v2/team")
        teams = r.json().get("teams", [])
        spaces = []

        print(f"📋 Found {len(teams)} teams:")

        for team in teams:
            try:
                team_name = team.get('name', 'Unknown Team')
                team_id = team['id']
                print(f"  🏢 Team: {team_name} (ID: {team_id})")

                r2 = safe_get(f"https://api.clickup.com/api/v2/team/{team_id}/space")
                team_spaces = r2.json().get("spaces", [])

                print(f"    📂 Spaces in this team ({len(team_spaces)}):")
                for space in team_spaces:
                    space_name = space.get('name', 'Unknown Space')
                    space_id = space.get('id', 'Unknown ID')
                    print(f"      - {space_name} (ID: {space_id})")

                spaces.extend(team_spaces)

            except Exception as e:
                logging.error(f"Error fetching spaces for team {team['id']}: {e}")
                error_log.append({"get_team_spaces": f"{team['id']}: {str(e)}"})

        print(f"\n✅ Total spaces found for backup: {len(spaces)}")
        return spaces
    except Exception as e:
        logging.error(f"Critical error while loading spaces: {e}")
        error_log.append({"get_spaces": str(e)})
        return []


# Deduplicate a list of dictionaries by the value of their "id" field.
# The last occurrence of a given ID wins.
def _dedupe_by_id(items):
    d = {}
    for it in items or []:
        iid = it.get("id")
        if iid:
            d[iid] = it
    return list(d.values())


# Fetch all folders for a space, including both archived and non-archived ones,
# and return a deduplicated result.
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


# Fetch all lists that belong to a folder, including archived and non-archived ones,
# and return a deduplicated result.
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


# Fetch all root-level lists directly under a space, including archived
# and non-archived ones, and return a deduplicated result.
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


# Fetch complete metadata for a single list.
# Returns None if the list does not exist or if the request fails.
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


# Fetch all tasks from a list, handling pagination and collecting both archived
# and non-archived tasks. Tasks are deduplicated by ID.
def get_tasks(list_id) -> list:
    # The request used to estimate task count was intentionally disabled because
    # it did not provide useful value and only consumed rate limit.
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/list/{list_id}/")
        if r.status_code == 200:
            task_count = r.json().get("task_count", 0)
        else:
            task_count = 0
    except:
        task_count = 0
    
    tasks_by_id = {}
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


# Fetch all comments for a task using pagination based on the last comment ID/date.
# The function walks back through older comment history until no more comments are returned.
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

        # Take ID + date from the last comment to page older history.
        last = batch[-1]
        last_id = last.get("id")
        last_date = last.get("date")
        if not last_id or not last_date:
            break

        params = {"start_id": last_id, "start": last_date}

    return all_comments


# Fetch full task details for a single task.
# Returns None if the task does not exist or if the request fails.
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


# Fetch attachment metadata for a task by reading the full task payload.
# Returns an empty list if the task does not exist or if the request fails.
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


# Compute how long the script should sleep before retrying a failed request.
# It prefers server-provided retry headers and falls back to exponential backoff.
def _retry_sleep_seconds(resp, attempt: int) -> int:
    # Prefer server guidance.
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1, int(float(ra)))  # sometimes Retry-After can be "1.0"
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


# Download a single attachment file with retry handling, streaming, progress display,
# disk space validation, and atomic replacement of the final file on success.
def download_attachment(url, dest_path, desc="Downloading"):
    global total_attachments_downloaded, total_size_mb

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    tmp_path = dest_path + ".part"
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
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

            # Disk space check (abort if insufficient).
            free_mb = check_current_disk_space(0, os.path.dirname(dest_path))
            needed_mb = (total_size / (1024 * 1024)) if total_size else 0
            if total_size and needed_mb > free_mb:
                err = f"not enough space. Available: {free_mb:.1f} MB, required: {needed_mb:.1f} MB"
                logging.error(f"Error downloading attachment {url} to {dest_path}: {err}")
                error_log.append({"download_attachment": f"{url}: {err}"})
                r.close()
                return False, 0.0

            # Stream to temp file first.
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

            # Atomic move into place.
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


# Safely write JSON data to a file by first writing to a temporary backup path
# and then atomically replacing the target file.
def safe_file_write(file_path, data) -> bool:
    backup_path = f"{file_path}.backup"
    try:
        # Create the directory if it does not exist.
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(backup_path, file_path)  # atomic operation
        return True
    except Exception as e:
        logging.error(f"Error writing to {file_path}: {e}")
        if os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except:
                pass
        return False


# Process a single list and collect all tasks, comments, attachments,
# and list-level metadata into a backup structure.
def process_list(lst, space_dir) -> dict:
    # This is effectively the main parser for list/task data.
    global total_tasks_processed

    list_id = lst.get("id")
    list_name = sanitize_filename(lst.get("name", "Unknown"))

    logging.info(f"    » Processing list '{list_name}' (ID: {list_id})")

    # Get complete list metadata.
    list_details = get_list_details(list_id)

    tasks = get_tasks(list_id)
    logging.info(f"      Found {len(tasks)} tasks")

    list_data = {
        "name": list_name,
        "id": list_id,
        "tasks": [],
        "processed_at": datetime.now().isoformat(),
        "task_count": len(tasks),
        # Add complete metadata for restore.
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

    # Progress bar for task processing.
    with tqdm(tasks, desc=f"Processing {list_name[:30]}", unit="tasks", leave=False) as pbar:
        for task in pbar:
            try:
                tid = task.get("id")
                task_name = task.get("name", "Unnamed Task")
                pbar.set_postfix_str(f"Current: {task_name[:20]}...")

                total_tasks_processed += 1

                # Get full task details (contains more data than list tasks).
                task_full = get_task_details(tid)
                if not task_full:
                    task_full = task  # fallback to basic data

                # Get comments.
                comments = get_comments(tid)

                # Get attachments with complete metadata.
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

                        # If the file already exists, add a timestamp.
                        if os.path.exists(dest):
                            name, ext = os.path.splitext(fname)
                            fname = f"{name}_{int(time.time())}{ext}"
                            dest = os.path.join(att_dir, fname)

                        success, size_mb = download_attachment(url, dest, f"DL: {fname[:15]}")
                        rel_path = os.path.relpath(dest, space_dir)  # e.g., attachments/12345/attid__name.pdf
                        # Store complete metadata for restore.
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

                # Store COMPLETE task data for disaster recovery.
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
                    # Comments and attachments.
                    "comments": comments,
                    "attachments": att_data
                }

                list_data["tasks"].append(task_backup)

            except Exception as ex:
                logging.error(f"Error processing task {tid}: {ex}")
                error_log.append({"process_task": f"{tid}: {traceback.format_exc()}"})

    logging.info(f"      ✅ List '{list_name}' completed: {len(tasks)} tasks, {total_attachments_size:.2f} MB attachments")
    return list_data


# Fetch complete metadata for a space.
# Returns None if the space does not exist or if the request fails.
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


# Fetch complete metadata for a folder.
# Returns None if the folder does not exist or if the request fails.
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


# Back up one complete space including its folders, lists, tasks, attachments,
# and metadata, and then write the main JSON backup file for that space.
def backup_space(space) -> None:
    space_id = space.get("id")
    space_name = sanitize_filename(space.get("name", "Unknown_Space"))

    # If TARGET_SPACE_NAME is set, filter by it.
    if TARGET_SPACE_NAME and space_name != TARGET_SPACE_NAME:
        return

    print(f"\n🚀 Starting backup of space: {space_name} (ID: {space_id})")
    logging.info(f"🚀 Starting backup of space: {space_name} (ID: {space_id})")

    # Get complete space details.
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
        # Complete space metadata for restore.
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

    # Process folders and their lists.
    folders = get_folders(space_id)
    print(f"  📂 Found {len(folders)} folders")
    logging.info(f"  Found {len(folders)} folders")

    for folder in tqdm(folders, desc="Processing folders", leave=False):
        folder_name = folder.get("name", "Unknown_Folder")
        folder_id = folder.get("id")
        print(f"  📂 Processing folder: {folder_name}")
        logging.info(f"  📂 Processing folder: {folder_name}")

        # Get complete folder metadata.
        folder_details = get_folder_details(folder_id)

        folder_data = {
            "name": folder_name,
            "id": folder_id,
            "lists": [],
            # Complete folder metadata for restore.
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

    # Process root lists (outside folders).
    root_lists = get_lists_from_space(space_id)
    print(f"  📋 Found {len(root_lists)} root lists")
    logging.info(f"  Found {len(root_lists)} root lists")

    for lst in tqdm(root_lists, desc="Processing root lists", leave=False):
        list_data = process_list(lst, space_dir)
        space_data["lists"].append(list_data)
        space_data["backup_stats"]["total_tasks"] += list_data["task_count"]

    space_data["backup_stats"]["total_lists"] += len(root_lists)
    space_data["backup_stats"]["total_attachments"] = total_attachments_downloaded

    # Save the main JSON file.
    json_path = os.path.join(space_dir, f"backup_{space_name}_{space_id}_{timestamp}.json")
    if safe_file_write(json_path, space_data):
        success_log.append(f"Backup completed for space: {space_name}")
        print(f"✅ Space '{space_name}' backup completed successfully")
        logging.info(f"✅ Space '{space_name}' backup completed successfully")
    else:
        error_log.append(f"Failed to write main backup file for space: {space_name}")
        print(f"❌ Failed to write main backup file for space: {space_name}")
        logging.error(f"❌ Failed to write main backup file for space: {space_name}")


# Generate a JSON summary report for the whole backup run,
# including aggregate counters, success log, and error log.
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


# Write text to a file atomically by first writing to a temporary file
# in the same directory and then replacing the target.
def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding=encoding) as tf:
        tf.write(text)
        tmp_name = tf.name
    os.replace(tmp_name, path)  # atomic rename on same filesystem


# Serialize JSON data and write it atomically using atomic_write_text().
# This reduces the risk of leaving partially written files behind after a crash.
def atomic_write_json(path: Path, data: Any, encoding: str = "utf-8") -> None:
    text = json.dumps(data, indent=2, ensure_ascii=False)
    atomic_write_text(path, text, encoding=encoding)


@dataclass
class ValidationIssue:
    severity: str       # "critical" or "noncritical"
    where: str          # e.g. "validate"
    file: str
    error: str


# Check whether a path exists, is a readable file, and contains at least some data.
# Returns a tuple (ok, error_message).
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


# Validate that a JSON file is readable and structurally valid.
# Smaller files are fully parsed; large files get a lighter structural check
# to avoid loading huge JSON documents into memory.
def _validate_json_file(p: Path, max_parse_bytes: int = 50_000_000) -> Tuple[bool, str]:
    ok, err = _is_readable_file(p)
    if not ok:
        return False, err

    try:
        size = p.stat().st_size
        if size <= max_parse_bytes:
            with p.open("r", encoding="utf-8") as f:
                json.load(f)
            return True, ""
        # Cheap check for very large JSON: starts/ends with JSON container.
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


# Validate key files inside the export directory and return a list of issues.
# Certain file names can be skipped when needed.
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


# Safely delete a directory tree only if it is inside the expected backup root.
# This is a guardrail against deleting the wrong path by mistake.
def _safe_rmtree(target: Path, root: Path) -> None:
    target_r = target.resolve()
    root_r = root.resolve()

    # Only delete inside root.
    if target_r == root_r or root_r not in target_r.parents:
        raise ValueError(f"Refusing to delete outside backup root: {target_r}")

    shutil.rmtree(target_r)


# Apply retention policy to backup run directories based on manifest status and timestamp.
# Successful and non-successful runs are kept in separate retention pools.
def enforce_retention(
    current_export_dir: Path,
    keep_success: int = 3,
    keep_non_success: int = 2,
) -> None:
    current_export_dir = current_export_dir.resolve()
    backup_root = current_export_dir.parent.resolve()

    runs: List[Tuple[Path, str, float]] = []

    for d in backup_root.iterdir():
        if not d.is_dir():
            continue

        manifest = d / "manifest.json"
        if not manifest.exists():
            continue  # safety: ignore unknown dirs

        status = "UNKNOWN"
        sort_ts = d.stat().st_mtime
        try:
            with manifest.open("r", encoding="utf-8") as f:
                m = json.load(f)

            status = str(m.get("run_status", "UNKNOWN"))

            ended_iso = m.get("ended_at_iso")
            if ended_iso:
                sort_ts = datetime.fromisoformat(ended_iso).timestamp()
            else:
                ended_ts = m.get("ended_at_ts")
                if isinstance(ended_ts, (int, float)):
                    sort_ts = float(ended_ts)

        except Exception as e:
            logging.warning(f"Retention: failed to parse manifest {manifest}: {e}")

        runs.append((d.resolve(), status, sort_ts))

    # Newest -> oldest.
    runs.sort(key=lambda x: x[2], reverse=True)

    successes = [r for r in runs if r[1] == "SUCCESS"]
    nons = [r for r in runs if r[1] != "SUCCESS"]

    keep_set = {p for (p, _, _) in successes[:keep_success]} | {p for (p, _, _) in nons[:keep_non_success]}

    # Always keep the current run no matter what (safety).
    keep_set.add(current_export_dir)

    for (p, status, _) in runs:
        if p in keep_set:
            continue
        if p == current_export_dir:
            continue  # extra safety
        try:
            logging.info(f"Retention: deleting old backup {p.name} (status={status})")
            _safe_rmtree(p, backup_root)
        except Exception as e:
            logging.error(f"Retention: failed to delete {p}: {e}")


# Main entry point for the complete backup workflow.
# It initializes logging, checks prerequisites, processes spaces,
# writes final reports, validates output, computes run status, and applies retention.
def main():
    setup_logging()

    print("🚀 Starting COMPLETE ClickUp backup process")
    print(f"📁 Export directory: {os.path.abspath(EXPORT_DIR)}")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    logging.info("🚀 Starting ClickUp backup process")
    logging.info(f"📁 Export directory: {os.path.abspath(EXPORT_DIR)}")

    # Disk space check.
    check_disk_space()

    start_time = time.time()

    # Get the list of spaces.
    spaces = get_spaces()
    if not spaces:
        print("❌ No spaces found or failed to fetch spaces!")
        logging.error("❌ No spaces found or failed to fetch spaces!")
        return

    print(f"\n📊 BACKUP PLAN:")
    print(f"   📂 Spaces to back up: {len(spaces)}")
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
            print(f"❌ Critical error while backing up space {space_name}: {e}")
            logging.error(f"Critical error while backing up space {space_name}: {e}")
            error_log.append({
                "severity": "critical",
                "scope": "space",
                "space_name": space_name,
                "space_id": space.get("id"),
                "where": "backup_space",
                "error": str(e),
                "trace": traceback.format_exc(),
            })

    # Final reports and statistics.
    end_time = time.time()
    duration = end_time - start_time

    # Generate summary report.
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
    print(f"📄 For docs: use manual export from the ClickUp interface")

    logging.info("✅ Backup process completed!")
    logging.info(f"📊 Duration: {duration/60:.1f} minutes")
    logging.info(f"📊 Stats: {len(success_log)} spaces, {total_tasks_processed} tasks, {total_attachments_downloaded} attachments")

    export_dir = Path(EXPORT_DIR)
    log_success_path = export_dir / "log_success.txt"
    log_errors_path = export_dir / "log_errors.json"

    if error_log:
        print(f"⚠️  There were {len(error_log)} errors. Check {log_errors_path} for details.")
        logging.warning(f"⚠️  There were {len(error_log)} errors. Check {log_errors_path} for details.")

    print(f"\n📋 Summary report: backup_summary.json")

    run_status: str = ""
    # FINALIZE: decide status, validate, write artifacts, retention.


    spaces_planned = len(spaces)

    # IMPORTANT: only correct if success_log is 1 entry per successful space.
    spaces_ok = len(success_log)
    spaces_failed = spaces_planned - spaces_ok

    # Compute total, critical, and noncritical error counts from error_log.
    def compute_counts() -> tuple[int, int, int]:
        errors_total = len(error_log)
        critical = sum(1 for e in error_log if e.get("severity") == "critical")
        noncritical = errors_total - critical
        return errors_total, critical, noncritical

    # Decide the final run status based on completed spaces and critical errors.
    def decide_status(spaces_ok: int, spaces_planned: int, critical_errors: int) -> str:
        if spaces_ok == 0 or critical_errors > 0:
            return "FAILED"
        if spaces_ok < spaces_planned:
            return "PARTIAL"
        return "SUCCESS"

    # 1) Initial status.
    errors_total, critical_errors, noncritical_errors = compute_counts()
    run_status = decide_status(spaces_ok, spaces_planned, critical_errors)
    
    atomic_write_text(log_success_path, "\n".join(success_log) + "\n")
    atomic_write_json(log_errors_path, error_log)

    # 2) Validate export dir and append issues.
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

    # 3) Recompute counts after appending validation issues.
    errors_total, critical_errors, noncritical_errors = compute_counts()
    run_status = decide_status(spaces_ok, spaces_planned, critical_errors)

    # 4) If validation found critical issues, force FAILED (belt-and-suspenders).
    if any(i.severity == "critical" for i in validation_issues):
        run_status = "FAILED"

    # 5) Write manifest + marker.
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

    # 7) Retention.
    if run_status == "SUCCESS":
        enforce_retention(export_dir, keep_success=3, keep_non_success=2)
    else:
        logging.info(f"Retention: skipped (run_status={run_status})")

    if run_status == "SUCCESS":
        return (0)
    elif run_status == "PARTIAL":
        return (2)
    else:
        return (1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n❌ Backup interrupted by user")
        # Attempt to save partial results.
        generate_summary_report()
        sys.exit(1)
    except Exception as e:
        logging.error(f"Critical error: {e}\n{traceback.format_exc()}")
        print(f"❌ Critical error: {e}")
        sys.exit(1)