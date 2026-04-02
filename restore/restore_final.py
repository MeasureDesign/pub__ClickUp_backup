import os
import sys
import json
import time
import logging
import random
from datetime import datetime
from pathlib import Path
import requests
from requests.exceptions import RequestException
from typing import Union
import mimetypes
import unicodedata
import re
from getpass import getpass


# Simple ANSI color helper class for colored terminal output.
# Example:
# print(f"{bcolors.HEADER}some_text{bcolors.ENDC}")
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# Ask the user for a ClickUp API token and validate it immediately.
# This keeps secrets out of the script and avoids shipping credentials with the code.
# The function keeps prompting until the token is valid or exits on network/request errors.
def get_api_token() -> str:
    print(f"{bcolors.HEADER}You have to enter your API key to authenticate{bcolors.ENDC}")
    print(f"{bcolors.WARNING}You can find your API key at: {bcolors.UNDERLINE}https://app.clickup.com/settings/apps{bcolors.ENDC}")
    while True:
        api_token: str = getpass("Enter ClickUp token: (input is hidden): ").strip()
        if len(api_token) < 40:
            print(f"{bcolors.FAIL}Invalid API token{bcolors.ENDC}")
            continue
        try:
            r = requests.get("https://api.clickup.com/api/v2/user", headers={"Authorization": api_token}, timeout=30)
            if r.status_code == 200:
                print(f"{bcolors.OKGREEN}token ok{bcolors.ENDC}")
                return (api_token)
            elif r.status_code in (401, 403):
                print("Invalid token, try again")
                continue
            else:
                print(f"Unexpected status: {r.status_code}")
                sys.exit(1)
        except requests.Timeout:
            print("Request timeout")
            sys.exit(1)
        except requests.RequestException as e:
            print(f"Network error: {e.__class__.__name__}, {e}")
            sys.exit(1)


# Find the newest backup folder in the given directory based on its timestamped name.
# The expected folder name format ends with YYYYMMDD_HHMMSS.
# This is used so the script can automatically work with the most recent backup run.
def newest_folder_by_name(base_dir: str | Path) -> Path:
    base = Path(base_dir)

    candidates = []
    for p in base.iterdir():
        if not p.is_dir():
            continue
        parts = p.name.split("_")
        if len(parts) < 2:
            continue
        # Expect ..._YYYYMMDD_HHMMSS at the end.
        date_part, time_part = parts[-2], parts[-1]
        try:
            ts = datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        candidates.append((ts, p))
    if not candidates:
        logging.error("No folders matching *_YYYYMMDD_HHMMSS found")
        raise FileNotFoundError("No folders matching *_YYYYMMDD_HHMMSS found")
    return max(candidates, key=lambda x: x[0])[1]


# API token for the account:
API_TOKEN = get_api_token()

HEADERS = {"Authorization": API_TOKEN}

# The working directory must contain at least one backup folder.
# If multiple backup folders exist, the script picks the newest one based on the timestamp in its name.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    BACKUP_DIR = os.path.join(SCRIPT_DIR, newest_folder_by_name(SCRIPT_DIR))
except FileNotFoundError as e:
    print("Backup folder not found.")
    sys.exit(1)

# Retry configuration.
MAX_RETRIES = 8
DEFAULT_RETRY_AFTER = 5
# Pause between downloading specific files in seconds.
DELAY_BETWEEN_FILES = 2
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

# Global cache for created structures.
created_statuses = {}  # list_id -> set of status names
created_tags = {}      # space_id -> set of tag names
workspace_structure = {}

# Complete statistics for success/error reporting and final exit summary.
uploaded_attachements = 0
total_files_processed = 0
total_spaces_imported = 0
total_lists_imported = 0
total_tasks_imported = 0
failed_imports = []
successful_imports = []
all_import_errors = []


# Configure runtime logging to both a file and the console.
# This makes the import easier to troubleshoot during long runs and preserves a persistent audit trail.
def setup_logging():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(SCRIPT_DIR, f"import_{timestamp}.log")

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    print(f"Batch import log: {log_file}")
    return timestamp


MAX_RETRIES = 8
DEFAULT_RETRY_AFTER = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


# Determine how long to sleep after a rate-limit response.
# ClickUp exposes X-RateLimit-Reset as a Unix timestamp, which is preferred.
# If that is missing, the function falls back to Retry-After, then to the next minute boundary.
def _sleep_seconds_for_rate_limit(resp) -> int:
    reset_ts = resp.headers.get("X-RateLimit-Reset")
    if reset_ts:
        try:
            return max(1, int(reset_ts) - int(time.time()) + 1)
        except ValueError:
            pass

    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1, int(float(ra)))
        except ValueError:
            pass

    # Fallback: ClickUp limits are per-minute; sleep to the next minute boundary (+1s).
    return (60 - datetime.now().second) + 1


# Determine how long to wait after transient server-side failures.
# Retry-After is preferred when present, otherwise exponential backoff is used with a cap.
def _sleep_seconds_for_server_error(resp, attempt: int) -> int:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1, int(float(ra)))
        except ValueError:
            pass

    return min(30, 2 ** attempt)  # Cap at 30 seconds.


# Send a POST request with retry handling tailored for ClickUp.
# This function centralizes rate-limit handling, transient 5xx retries, and network backoff
# so the rest of the import logic can call POST safely without duplicating retry code.
def safe_post(url, data=None, json_data=None, *, timeout=30, session: requests.Session | None = None):
    if data is not None and json_data is not None:
        raise ValueError("Pass only one of data= or json_data= (not both).")

    last_exception = None
    s = session or requests

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Important: use `is not None`, not truthiness, because {} is a valid payload.
            if json_data is not None:
                resp = s.post(url, headers=HEADERS, json=json_data, timeout=timeout)
            else:
                resp = s.post(url, headers=HEADERS, data=data, timeout=timeout)

            # Retry on rate limits and transient server errors.
            if resp.status_code in RETRY_STATUS_CODES:
                limit_remaining = resp.headers.get("X-RateLimit-Remaining")
                reset_ts = resp.headers.get("X-RateLimit-Reset")

                is_rl = (
                    resp.status_code == 429 or
                    (limit_remaining is not None and limit_remaining.isdigit() and int(limit_remaining) <= 0)
                )

                if is_rl:
                    sleep_s = _sleep_seconds_for_rate_limit(resp)
                    logging.warning(
                        f"Rate limit (status={resp.status_code}, remaining={limit_remaining}, reset={reset_ts}) "
                        f"-> sleeping {sleep_s}s ({attempt}/{MAX_RETRIES})"
                    )
                    # Small jitter helps prevent multiple workers from waking at the same instant.
                    time.sleep(sleep_s + random.uniform(0, 0.5))
                    continue

                # Retry transient 5xx errors.
                sleep_s = _sleep_seconds_for_server_error(resp, attempt)
                logging.warning(
                    f"Server error {resp.status_code} -> retry in {sleep_s}s ({attempt}/{MAX_RETRIES})"
                )
                time.sleep(sleep_s + random.uniform(0, 0.5))
                continue

            # Treat all 2xx POST responses as success.
            if 200 <= resp.status_code < 300:
                return resp

            # Non-retriable error.
            resp.raise_for_status()
            return resp

        except RequestException as e:
            last_exception = e
            # POST is not inherently idempotent.
            # Retrying after a timeout can create duplicates, but this is accepted here for restore use cases.
            backoff = min(30, 2 ** attempt)
            logging.error(f"POST network/HTTP error for {url}: {e} (attempt {attempt}/{MAX_RETRIES}), backoff={backoff}s")
            if attempt < MAX_RETRIES:
                time.sleep(backoff + random.uniform(0, 0.5))
                continue

    logging.error(f"Failed to POST {url} after {MAX_RETRIES} attempts")
    raise last_exception


# Send a GET request with retry handling for rate limits, server errors, and temporary network issues.
# This wrapper exists so all read operations follow one consistent error-handling policy.
def safe_get(url, params=None) -> object:
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)

            # Retry on rate limits and transient server errors.
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

            # A 404 can be valid in some flows, so return it instead of failing.
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


# Extract the source ClickUp team_id from the backup data itself.
# This is preferred because it keeps the restore tied to the original backup metadata.
def extract_source_team_id(space_backup: dict) -> str | None:
    team_ids = set()
    for lst in space_backup.get("lists", []):
        for t in lst.get("tasks", []):
            if t.get("team_id"):
                team_ids.add(str(t["team_id"]))
    if not team_ids:
        return None
    if len(team_ids) > 1:
        logging.warning(f"Multiple team_id values found in backup: {sorted(team_ids)}")
    return sorted(team_ids)[0]


# Determine which ClickUp team/workspace the import should target.
# The function first tries to read the team_id from the backup file and only falls back to the API if needed.
def get_team_id(space_backup: dict) -> str | None:
    team_ids = extract_source_team_id(space_backup)
    if team_ids is not None:
        return (team_ids)
    else:
        logging.error(f"Failed to find team id in backup file")
        try:
            r = safe_get("https://api.clickup.com/api/v2/team")
            if r:
                teams = r.json().get("teams", [])
                if teams:
                    return teams[0]["id"]
            return None
        except Exception as e:
            logging.error(f"Error getting team ID: {e}")
            return None


# Extract all user references that appear across the backup.
# The result is a normalized mapping keyed by email when available, otherwise by id,
# so later logic can map assignees and preserve attribution as accurately as possible.
def extract_users(space_backup: dict) -> dict:
    users = {}

    def upsert(user_obj: dict | None):
        if not user_obj:
            return
        uid = user_obj.get("id")
        email = user_obj.get("email")
        username = user_obj.get("username")

        key = email or (f"id:{uid}" if uid is not None else None)
        if not key:
            return

        entry = users.setdefault(key, {"email": None, "id": None, "username": None})
        if email and not entry["email"]:
            entry["email"] = email
        if uid is not None and not entry["id"]:
            entry["id"] = str(uid)
        if username and not entry["username"]:
            entry["username"] = username

    # Walk known backup structures where user references can appear.
    for lst in space_backup.get("lists", []):
        for t in lst.get("tasks", []):
            upsert(t.get("creator"))
            for a in t.get("assignees", []):
                upsert(a)
            for w in t.get("watchers", []):
                upsert(w)

            for c in t.get("comments", []):
                upsert(c.get("user"))

            for att in t.get("attachments", []):
                upsert(att.get("user"))

            # Optional: inspect custom field values that may store users.
            for cf in t.get("custom_fields", []):
                if cf.get("type") == "users":
                    val = cf.get("value")
                    if isinstance(val, list):
                        for u in val:
                            if isinstance(u, dict):
                                upsert(u)
                    elif isinstance(val, dict):
                        upsert(val)

    return users


# Fetch the current workspace users from the ClickUp API.
# This is used to map backup users to users that already exist in the destination workspace.
def get_workspace_users(team_id: int, space_backup: dict) -> dict | None:
    try:
        r = safe_get(f"https://api.clickup.com/api/v2/team/{team_id}/member")
        if r and r.status_code == 200:
            members = r.json().get('members', [])
            user_mapping = {}
            for member in members:
                user_data = member.get('user', {})
                email = user_data.get('email')
                user_id = user_data.get('id')
                username = user_data.get('username', 'Unknown')

                if email and user_id:
                    user_mapping[email] = {
                        'id': user_id,
                        'username': username
                    }

            logging.info(f"Found {len(user_mapping)} users in workspace(API call)")
            return user_mapping
        else:
            logging.warning("Failed to get workspace members")
            return {}
    except Exception as e:
        logging.error(f"Error getting workspace users: {e}")
        return {}


# Analyze all selected backup files before import starts.
# This creates a high-level overview of structures, users, statuses, tags, and priorities
# so the operator can understand what will be restored and what may require manual preparation.
def comprehensive_backup_analysis(backup_files):
    print("\n🔍 COMPREHENSIVE ANALYSIS OF BACKUP FILES...")

    analysis = {
        "users": {},           # user_id -> {username, email}
        "statuses": set(),     # all unique statuses
        "tags": set(),         # all unique tags
        "priorities": set(),   # all priorities
        "spaces": [],          # information about spaces
        "custom_fields": {},   # custom fields by type
        "file_count": len(backup_files),
        "estimated_structures": {
            "spaces": 0,
            "folders": 0,
            "lists": 0,
            "tasks": 0
        }
    }

    for i, backup_file in enumerate(backup_files):
        try:
            print(f"   Analyzing [{i+1}/{len(backup_files)}]: {os.path.basename(backup_file)}")

            backup_data = load_backup_file(backup_file)
            if not backup_data:
                continue

            # Basic space information.
            space_info = {
                "name": backup_data.get("name", "Unknown"),
                "file": os.path.basename(backup_file),
                "folders": len(backup_data.get("folders", [])),
                "root_lists": len(backup_data.get("lists", [])),
                "total_lists": 0,
                "total_tasks": 0
            }

            # Walk all lists, both inside folders and at the root of the space.
            all_lists = backup_data.get("lists", [])
            for folder in backup_data.get("folders", []):
                all_lists.extend(folder.get("lists", []))

            space_info["total_lists"] = len(all_lists)
            analysis["estimated_structures"]["spaces"] += 1
            analysis["estimated_structures"]["folders"] += len(backup_data.get("folders", []))
            analysis["estimated_structures"]["lists"] += len(all_lists)

            # Analyze every list.
            for list_data in all_lists:
                tasks = list_data.get("tasks", [])
                space_info["total_tasks"] += len(tasks)
                analysis["estimated_structures"]["tasks"] += len(tasks)

                # Analyze every task.
                for task in tasks:
                    # Users.
                    for assignee in task.get("assignees", []):
                        if isinstance(assignee, dict):
                            user_id = assignee.get("id")
                            username = assignee.get("username", "Unknown")
                            email = assignee.get("email", "")
                            if user_id:
                                analysis["users"][user_id] = {
                                    "username": username,
                                    "email": email
                                }

                    # Statuses.
                    status = task.get("status", {})
                    if isinstance(status, dict):
                        status_name = status.get("status")
                        if status_name:
                            analysis["statuses"].add(status_name)
                    elif isinstance(status, str) and status:
                        analysis["statuses"].add(status)

                    # Tags.
                    for tag in task.get("tags", []):
                        if isinstance(tag, dict) and tag.get("name"):
                            analysis["tags"].add(tag["name"])
                        elif isinstance(tag, str) and tag.strip():
                            analysis["tags"].add(tag.strip())

                    # Priorities.
                    priority = task.get("priority")
                    if priority:
                        if isinstance(priority, dict):
                            priority_name = priority.get("priority")
                            if priority_name:
                                analysis["priorities"].add(priority_name)
                        elif isinstance(priority, str):
                            analysis["priorities"].add(priority)

                    # Custom fields for future extension and visibility.
                    custom_fields = task.get("custom_fields", [])
                    for cf in custom_fields:
                        if isinstance(cf, dict):
                            cf_type = cf.get("type")
                            cf_name = cf.get("name")
                            if cf_type and cf_name:
                                if cf_type not in analysis["custom_fields"]:
                                    analysis["custom_fields"][cf_type] = set()
                                analysis["custom_fields"][cf_type].add(cf_name)

            analysis["spaces"].append(space_info)

        except Exception as e:
            logging.warning(f"Error analyzing backup file {backup_file}: {e}")
            continue

    # Convert sets to lists for JSON serialization and consistent display.
    analysis["statuses"] = sorted(list(analysis["statuses"]))
    analysis["tags"] = sorted(list(analysis["tags"]))
    analysis["priorities"] = sorted(list(analysis["priorities"]))

    for cf_type in analysis["custom_fields"]:
        analysis["custom_fields"][cf_type] = sorted(list(analysis["custom_fields"][cf_type]))

    return analysis


# Print a readable summary of the pre-import analysis.
# This gives the operator a checklist of what exists in the backup and what may need manual handling,
# especially custom statuses and user mapping gaps.
def display_analysis_summary(analysis, user_mapping):
    print(f"\n📊 ANALYSIS RESULTS:")
    print(f"=" * 60)

    # Basic statistics.
    print(f"📁 FILES AND STRUCTURES:")
    print(f"   • Backup files: {analysis['file_count']}")
    print(f"   • Spaces to create: {analysis['estimated_structures']['spaces']}")
    print(f"   • Total folders: {analysis['estimated_structures']['folders']}")
    print(f"   • Total lists: {analysis['estimated_structures']['lists']}")
    print(f"   • Total tasks: {analysis['estimated_structures']['tasks']}")

    # Structural elements important for manual setup.
    print(f"\n🔧 ELEMENTS REQUIRING MANUAL SETUP:")
    print(f"   • Custom statuses: {len(analysis['statuses'])}")
    if analysis['statuses']:
        print(f"     → {', '.join(analysis['statuses'][:10])}")
        if len(analysis['statuses']) > 10:
            print(f"     → ... and {len(analysis['statuses']) - 10} more")
        print(f"     ⚠️  CREATE THESE STATUSES MANUALLY IN EACH LIST!")

    print(f"   • Tags: {len(analysis['tags'])}")
    if analysis['tags']:
        print(f"     → {', '.join(analysis['tags'][:10])}")
        if len(analysis['tags']) > 10:
            print(f"     → ... and {len(analysis['tags']) - 10} more")
        print(f"     ℹ️  Tags will be created automatically on first use")

    print(f"   • Priorities: {len(analysis['priorities'])}")
    if analysis['priorities']:
        print(f"     → {', '.join(analysis['priorities'])}")

    # User mapping.
    print(f"\n👥 USER MAPPING:")
    mapped_users = []
    unmapped_users = []

    for user_id, user_info in analysis['users'].items():
        email = user_info.get('email', '')
        username = user_info.get('username', 'Unknown')

        if email and email in user_mapping:
            mapped_users.append(f"{username} ({email})")
        else:
            unmapped_users.append(f"{username} ({email})")

    print(f"   • Total users in backup: {len(analysis['users'])}")
    print(f"   • Mapped: {len(mapped_users)}")
    print(f"   • Unmapped: {len(unmapped_users)}")

    if mapped_users:
        print(f"   • Successfully mapped:")
        for user in mapped_users[:5]:
            print(f"     ✓ {user}")
        if len(mapped_users) > 5:
            print(f"     ✓ ... and {len(mapped_users) - 5} more")

    if unmapped_users:
        print(f"   • Unmapped (will be noted in description):")
        for user in unmapped_users[:5]:
            print(f"     ⚠ {user}")
        if len(unmapped_users) > 5:
            print(f"     ⚠ ... and {len(unmapped_users) - 5} more")

    # Space details.
    print(f"\n📦 SPACE DETAILS:")
    for space in analysis['spaces'][:5]:
        print(f"   • {space['name']}")
        print(f"     → Folders: {space['folders']}, Lists: {space['total_lists']}, Tasks: {space['total_tasks']}")

    if len(analysis['spaces']) > 5:
        print(f"   • ... and {len(analysis['spaces']) - 5} more spaces")

    # Rough time estimate.
    estimated_minutes = analysis['estimated_structures']['tasks'] * 0.05
    print(f"\n⏱ IMPORT ESTIMATE:")
    print(f"   • Estimated time: {estimated_minutes:.1f} minutes (without automatic statuses)")
    print(f"   • Recommended pause between files: {DELAY_BETWEEN_FILES}s")

    # Manual setup instructions.
    if analysis['statuses']:
        print(f"\n📋 MANUAL SETUP INSTRUCTIONS:")
        print(f"   1. After creating each list, go to the ClickUp UI")
        print(f"   2. In each list create these custom statuses:")
        print(f"      {', '.join(analysis['statuses'])}")
        print(f"   3. Then continue with the import - tasks will be assigned to statuses correctly")


# Placeholder for automatic custom status creation.
# It is intentionally disabled, because statuses are created manually in the ClickUp UI in this workflow.
def create_custom_status(list_id, status_name, status_type="custom"):
    # Automatic creation of statuses is disabled - statuses are set manually.
    logging.debug(f"Skipping automatic status creation: {status_name} (will be created manually)")
    return False


# Prepare the list structure before task import.
# In this version it only logs which statuses are needed, because custom statuses are expected
# to be created manually by the operator instead of through the API.
def pre_setup_list_structure(list_id, list_name, required_statuses):
    # Automatic status setup is disabled.
    # The user will create statuses manually in the ClickUp UI.
    if required_statuses:
        logging.info(f"List {list_name} will need these custom statuses (create manually): {', '.join(required_statuses[:5])}")
        if len(required_statuses) > 5:
            logging.info(f"... and {len(required_statuses) - 5} more statuses")

    return 0  # No statuses were created automatically.


# Create a new ClickUp space for one imported backup.
# The naming includes a timestamp and optional index so imported spaces are easy to identify and do not overwrite originals.
def create_space(team_id, space_name, file_index=None):
    try:
        # Improved naming with a shorter prefix.
        timestamp = datetime.now().strftime('%m%d_%H%M')
        if file_index is not None:
            prefixed_name = f"IMP_{space_name}_{file_index:02d}_{timestamp}"
        else:
            prefixed_name = f"IMP_{space_name}_{timestamp}"

        # Shorten the name if it exceeds ClickUp's length limit.
        if len(prefixed_name) > 50:
            space_name_short = space_name[:20] + "..."
            prefixed_name = f"IMP_{space_name_short}_{file_index:02d}_{timestamp}"

        data = {
            "name": prefixed_name,
            "features": {
                "due_dates": {"enabled": True},
                "time_tracking": {"enabled": True},
                "tags": {"enabled": True},
                "priorities": {"enabled": True},
                "custom_fields": {"enabled": True},
                "dependency_warning": {"enabled": True},
                "portfolios": {"enabled": True}
            }
        }

        logging.info(f"Creating space: {prefixed_name}")
        r = safe_post(f"https://api.clickup.com/api/v2/team/{team_id}/space", json_data=data)

        if r and r.status_code in [200, 201]:
            space_data = r.json()
            space_id = space_data.get("id")
            logging.info(f"Space created: {prefixed_name} (ID: {space_id})")
            return space_id, prefixed_name
        else:
            logging.error(f"Failed to create space. Status: {r.status_code if r else 'None'}")
            return None, None

    except Exception as e:
        logging.error(f"Error creating space {space_name}: {e}")
        return None, None


# Create a folder inside a destination ClickUp space.
# This preserves the folder hierarchy from the original backup structure.
def create_folder(space_id, folder_name):
    try:
        data = {"name": folder_name}

        logging.info(f"Creating folder: {folder_name}")
        r = safe_post(f"https://api.clickup.com/api/v2/space/{space_id}/folder", json_data=data)

        if r and r.status_code in [200, 201]:
            folder_data = r.json()
            folder_id = folder_data.get("id")
            logging.info(f"Folder created: {folder_name} (ID: {folder_id})")
            return folder_id
        else:
            logging.error(f"Failed to create folder {folder_name}")
            return None

    except Exception as e:
        logging.error(f"Error creating folder {folder_name}: {e}")
        return None


# Create a ClickUp list under either a folder or a space.
# The function also logs which custom statuses must be created manually before tasks are restored into the list.
def create_list(parent_type, parent_id, list_name, required_statuses=None):
    try:
        data = {"name": list_name}

        if parent_type == "folder":
            url = f"https://api.clickup.com/api/v2/folder/{parent_id}/list"
        else:  # space
            url = f"https://api.clickup.com/api/v2/space/{parent_id}/list"

        logging.info(f"Creating list: {list_name}")
        r = safe_post(url, json_data=data)

        if r and r.status_code in [200, 201]:
            list_data = r.json()
            list_id = list_data.get("id")
            logging.info(f"List created: {list_name} (ID: {list_id})")

            # Log information about required statuses that need to be created manually.
            if required_statuses:
                pre_setup_list_structure(list_id, list_name, required_statuses)
                logging.info(f"List {list_name} ready - {len(required_statuses)} custom statuses need manual creation")
            else:
                logging.info(f"List {list_name} ready - no custom statuses needed")

            return list_id
        else:
            logging.error(f"Failed to create list {list_name}")
            return None

    except Exception as e:
        logging.error(f"Error creating list {list_name}: {e}")
        return None


# Map original assignees from the backup to destination ClickUp user IDs by email.
# This keeps task ownership as accurate as possible when the same users exist in the target workspace.
def map_assignees_by_email(old_assignees, user_mapping):
    # Map original assignees to new IDs by email.
    new_assignees = []
    unmapped_users = []

    for assignee in old_assignees:
        email = assignee.get('email', '').strip()
        username = assignee.get('username', 'Unknown')

        if email and email in user_mapping:
            new_assignees.append(user_mapping[email]['id'])
            logging.debug(f"Mapped user {username} ({email}) to new ID: {user_mapping[email]['id']}")
        else:
            unmapped_users.append(f"{username} ({email})")

    if unmapped_users:
        logging.debug(f"Could not map users: {', '.join(unmapped_users)}")

    return new_assignees, unmapped_users


# Convert priority values from backup format into the numeric ClickUp priority scale.
# This allows the import to accept ints, floats, dicts, or strings while still producing a valid API payload.
def convert_priority(priority_data):
    if not priority_data:
        return None

    # Already numeric.
    if isinstance(priority_data, (int, float)):
        return max(1, min(4, int(priority_data)))

    # Dict with a "priority" field.
    if isinstance(priority_data, dict):
        priority_str = priority_data.get("priority", "")
    else:
        priority_str = str(priority_data)

    # Mapping of textual priorities to ClickUp's numeric values.
    priority_map = {
        "urgent": 1,
        "high": 2,
        "normal": 3,
        "low": 4,
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4
    }

    return priority_map.get(priority_str.lower(), 3)


# Normalize timestamp values into epoch milliseconds.
# Backup data can contain strings, seconds, milliseconds, or even microseconds,
# so this helper makes date fields consistent before sending them to ClickUp.
def to_epoch_ms(v):
    if v is None:
        return None

    if isinstance(v, str):
        v = v.strip()
        if not v or v.lower() == "null":
            return None
        if not v.isdigit():
            raise ValueError(f"Bad timestamp string: {v!r}")
        v = int(v)

    if not isinstance(v, int):
        raise TypeError(f"Bad timestamp type: {type(v)}")

    # Seconds -> milliseconds.
    if v < 10_000_000_000:
        v *= 1000

    # Microseconds -> milliseconds.
    if v > 10_000_000_000_000_000:
        v //= 1000

    return v


# Normalize free-text status names for matching.
# This makes comparisons tolerant to whitespace and case differences.
def norm(s: str) -> str:
    return " ".join(s.strip().lower().split())


# Build a lookup table of destination statuses keyed by normalized status name.
# This helps map source task statuses to the exact status labels that exist in the target list.
def build_status_map(dest_statuses: list[dict]) -> dict[str, str]:
    # Destination status items typically look like: {"status": "...", "type": "...", "id": "...", ...}
    return {norm(st["status"]): st["status"] for st in dest_statuses if st.get("status")}


SYNONYMS = {
    "not started": ["to do", "todo", "open"],
    "to do": ["todo", "not started", "open"],

    "in progress": ["doing"],

    "done": ["closed", "complete", "completed"],
    "completed": ["complete", "done", "closed"],
    "complete": ["completed", "done", "closed"],
}


# Map a source status name to an existing destination status.
# Exact normalized matches are preferred, and configured synonyms are used as a fallback.
def map_status(src_status: str, dest_map: dict[str, str]) -> str | None:
    key = norm(src_status)
    if key in dest_map:
        return dest_map[key]

    for alt in SYNONYMS.get(key, []):
        alt_key = norm(alt)
        if alt_key in dest_map:
            return dest_map[alt_key]

    return None


# Create a ClickUp task from backup data and enrich its description with metadata that cannot be restored directly.
# This is the core task restore function: it handles fields like priority, dates, assignees, tags, comments,
# attachments, and fallback documentation when something cannot be mapped exactly.
def create_task(list_id, task_data, user_mapping=None, attachments_dir=None, parent_task_id=None):
    # Create a task, with statuses mapped when possible.
    statuses = []

    r = safe_get(f"https://api.clickup.com/api/v2/list/{list_id}")
    if r and r.status_code == 200:
        statuses = r.json().get("statuses", []) or []
    else:
        logging.debug(f"Could not fetch list statuses for {list_id}: {r.status_code if r else 'no response'}")

    dest_map = build_status_map(statuses)

    try:
        # Basic task payload.
        data = {
            "name": task_data.get("name", "Imported Task")
        }

        if parent_task_id:
            data["parent"] = parent_task_id

        # Description handling.
        original_description = task_data.get("description", "")
        additional_info = []

        # Priority.
        priority = convert_priority(task_data.get("priority"))
        if priority:
            data["priority"] = priority

        # Status: set directly if it exists in the target list, otherwise preserve it in the description.
        original_status = task_data.get("status")
        status_name = None
        if original_status:
            if isinstance(original_status, dict):
                status_name = original_status.get("status")
            else:
                status_name = str(original_status)

        if status_name:
            mapped = map_status(status_name, dest_map) if dest_map else None
            if mapped:
                data["status"] = mapped
                logging.debug(f"Status mapped: '{status_name}' -> '{mapped}'")
            else:
                additional_info.append(f"Original status: {status_name} (not found in destination list)")
                logging.debug(f"Status '{status_name}' not found in destination list; added to description only")

        # Due date and start date.
        due_date = to_epoch_ms(task_data.get("due_date"))
        if due_date:
            data["due_date"] = due_date
            data["due_date_time"] = False

        start_date = to_epoch_ms(task_data.get("start_date"))
        if start_date:
            data["start_date"] = start_date
            data["start_date_time"] = False

        logging.debug(f"Payload dates: start={data.get('start_date')} due={data.get('due_date')}")

        # Time estimate.
        time_estimate = task_data.get("time_estimate")
        if time_estimate and str(time_estimate).isdigit() and int(time_estimate) > 0:
            data["time_estimate"] = int(time_estimate)

        # Assignees.
        assignees = task_data.get("assignees", [])
        unmapped_users = []
        if assignees and user_mapping:
            new_assignees, unmapped_users = map_assignees_by_email(assignees, user_mapping)
            # new_assignees, unmapped_users = map_assignees_force_single_user(assignees, user_mapping)
            if new_assignees:
                data["assignees"] = new_assignees

        # Tags.
        tag_names = []
        tags = task_data.get("tags", [])
        if tags:
            for tag in tags:
                if isinstance(tag, dict) and "name" in tag and tag["name"]:
                    tag_names.append(str(tag["name"]).strip())
                elif isinstance(tag, str) and tag.strip():
                    tag_names.append(tag.strip())

            # Remove duplicates and empty tags.
            tag_names = list(set([tag for tag in tag_names if tag]))
            if tag_names:
                data["tags"] = tag_names

        # Add non-restorable metadata to the description.
        # Assignees.
        all_assignees = task_data.get("assignees", [])
        if all_assignees:
            assignee_names = []
            for assignee in all_assignees:
                if isinstance(assignee, dict):
                    username = assignee.get('username', 'Unknown')
                    assignee_names.append(username)
            if assignee_names:
                additional_info.append(f"Assignees: {', '.join(assignee_names)}")

        if unmapped_users:
            additional_info.append(f"Unmapped users: {', '.join(unmapped_users)}")

        # Tags.
        if tags and tag_names:
            additional_info.append(f"Tags: {', '.join(tag_names)}")

        # Watchers.
        watchers = task_data.get("watchers", [])
        if watchers:
            watcher_names = []
            for watcher in watchers:
                if isinstance(watcher, dict):
                    watcher_names.append(watcher.get('username', 'Unknown'))
            if watcher_names:
                additional_info.append(f"Watchers: {', '.join(watcher_names)}")

        # Time tracked.
        time_spent = task_data.get("time_spent")
        if time_spent and int(time_spent) > 0:
            hours = int(time_spent) // 3600000
            minutes = (int(time_spent) % 3600000) // 60000
            additional_info.append(f"Time tracked: {hours}h {minutes}m")

        # Dependencies.
        dependencies = task_data.get("dependencies", [])
        if dependencies:
            additional_info.append(f"Dependencies: {len(dependencies)} task(s)")

        # Linked tasks.
        linked_tasks = task_data.get("linked_tasks", [])
        if linked_tasks:
            additional_info.append(f"Linked tasks: {len(linked_tasks)} task(s)")

        # Custom fields.
        custom_fields = task_data.get("custom_fields", [])
        if custom_fields:
            cf_info = []
            for cf in custom_fields:
                if isinstance(cf, dict):
                    cf_name = cf.get("name", "Unknown field")
                    cf_value = cf.get("value")
                    if cf_value:
                        cf_info.append(f"{cf_name}: {cf_value}")
            if cf_info:
                additional_info.append("Custom fields:")
                for cf in cf_info:
                    additional_info.append(f"  • {cf}")

        # Original folder/list info.
        folder = task_data.get("folder", {})
        if folder and isinstance(folder, dict):
            folder_name = folder.get("name")
            if folder_name:
                additional_info.append(f"Original folder: {folder_name}")

        list_info = task_data.get("list", {})
        if list_info and isinstance(list_info, dict):
            list_name = list_info.get("name")
            if list_name:
                additional_info.append(f"Original list: {list_name}")

        if task_data.get("date_created"):
            try:
                created_timestamp = int(task_data["date_created"])
                if created_timestamp > 10000000000:
                    created_timestamp = created_timestamp // 1000
                created_date = datetime.fromtimestamp(created_timestamp)
                additional_info.append(f"Originally created: {created_date.strftime('%Y-%m-%d %H:%M')}")
            except Exception as e:
                logging.debug(f"Could not parse date_created: {e}")

        if task_data.get("date_updated"):
            try:
                updated_timestamp = int(task_data["date_updated"])
                if updated_timestamp > 10000000000:
                    updated_timestamp = updated_timestamp // 1000
                updated_date = datetime.fromtimestamp(updated_timestamp)
                additional_info.append(f"Last updated: {updated_date.strftime('%Y-%m-%d %H:%M')}")
            except Exception as e:
                logging.debug(f"Could not parse date_updated: {e}")

        # Final description assembly.
        final_description_parts = []
        if original_description and original_description.strip():
            final_description_parts.append(original_description.strip())

        if additional_info:
            final_description_parts.append("---")
            final_description_parts.extend(additional_info)

        if final_description_parts:
            data["description"] = "\n".join(final_description_parts)

        # Send the task create request.
        logging.debug(f"Creating task: {data['name']}")
        r = safe_post(f"https://api.clickup.com/api/v2/list/{list_id}/task", json_data=data)

        if r and r.status_code in [200, 201]:
            new_task = r.json()
            task_id = new_task.get("id")
            logging.debug(f"Task created: {data['name']} (ID: {task_id})")

            # Restore comments after the task itself exists.
            comments = task_data.get("comments", [])
            if comments:
                create_comments(task_id, comments)

            # Upload attachments after the task itself exists.
            attachments = task_data.get("attachments", [])
            if attachments and attachments_dir:
                uploaded_count = 0
                for att in attachments:
                    if upload_attachment(task_id, att, attachments_dir):
                        uploaded_count += 1
                    time.sleep(0.5)  # Small delay between uploads.

                if uploaded_count > 0:
                    logging.info(f"✅ Uploaded {uploaded_count}/{len(attachments)} attachments for task {task_id}")

            return task_id
        else:
            logging.error(f"Failed to create task: {data['name']} - Response: {r.text if r else 'No response'}")
            return None

    except Exception as e:
        logging.error(f"Error creating task: {e}")
        return None


# Sanitize a filename before uploading it to ClickUp.
# This removes directory components, normalizes Unicode, restricts unsafe characters,
# and ensures the final name is reasonably short and API-safe.
def sanitize_upload_filename(name: str, fallback: str = "file") -> str:
    # Drop any directory components.
    name = Path(name).name

    # Normalize Unicode to an ASCII-friendly form.
    base, ext = os.path.splitext(name)
    base = unicodedata.normalize("NFKD", base).encode("ascii", "ignore").decode("ascii") or fallback

    # Allow only safe characters.
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-") or fallback

    # Keep the extension if it looks safe.
    ext = ext.lower()
    if ext and not re.fullmatch(r"\.[a-z0-9]{1,10}", ext):
        ext = ""

    out = (base + ext)[:180]
    return out


# Upload one attachment file to a ClickUp task.
# This uses the local file referenced in the backup metadata and preserves the original title when possible.
def upload_attachment(task_id: str, attachment_data: dict, attachments_dir: str, session=None) -> bool:
    session = session or requests.Session()
    global uploaded_attachements

    local_file = attachment_data.get("local_file")
    if not local_file:
        return False

    file_path = Path(attachments_dir) / local_file
    if not file_path.exists():
        return False

    # Prefer the original attachment title, otherwise use the stored filename.
    desired_name = attachment_data.get("title") or file_path.name
    upload_name = sanitize_upload_filename(desired_name)

    mime, _ = mimetypes.guess_type(upload_name)
    if not mime:
        mime = "application/octet-stream"

    url = f"https://api.clickup.com/api/v2/task/{task_id}/attachment"
    headers = {"Authorization": API_TOKEN}  # Do not set Content-Type manually.

    with file_path.open("rb") as f:
        files = {
            # Must be exactly "attachment".
            "attachment": (upload_name, f, mime),
        }

        r = session.post(url, headers=headers, files=files, timeout=60)

    if r.status_code in (200, 201):
        uploaded_attachements += 1
        return True

    # Useful debugging output when uploads fail.
    logging.warning(f"Failed to upload {upload_name}: {r.status_code} - {r.text}")
    return False


# Extract a comment timestamp in milliseconds for sorting.
# Invalid or missing timestamps resolve to 0 so comments can still be processed safely.
def _comment_ts_ms(c: dict) -> int:
    v = c.get("date", 0)
    try:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return 0
            v = int(v)
        elif isinstance(v, (float,)):
            v = int(v)
        elif not isinstance(v, int):
            return 0
        return v if v > 0 else 0
    except Exception:
        return 0


# Recreate comments on a destination task in chronological order.
# The author and timestamp are preserved in the comment body so historical context is not lost.
def create_comments(task_id, comments, max_comments=None):
    if not comments:
        return

    # Sort oldest -> newest.
    comments_sorted = sorted(comments, key=_comment_ts_ms)

    if max_comments is None:
        max_comments = len(comments_sorted)

    try:
        for comment in comments_sorted[:max_comments]:
            comment_text = comment.get("comment_text", "")
            if not comment_text:
                continue

            user = comment.get("user", {}) or {}
            author = user.get("username", "Unknown user")

            try:
                ts = _comment_ts_ms(comment)
                comment_date = datetime.fromtimestamp(ts / 1000) if ts else None
                formatted_date = comment_date.strftime('%Y-%m-%d %H:%M') if comment_date else "Unknown date"
            except Exception:
                formatted_date = "Unknown date"

            # comment_text = neutralize_mentions(comment_text)

            full_comment = f"[{author} - {formatted_date}]\n{comment_text}"
            data = {"comment_text": full_comment,
                    "notify_all": True}

            r = safe_post(f"https://api.clickup.com/api/v2/task/{task_id}/comment", json_data=data)
            if r and r.status_code in (200, 201):
                logging.debug(f"Comment added to task {task_id}")
            else:
                logging.warning(f"Failed to add comment to task {task_id}: {r.status_code if r else 'No response'} - {r.text if r else ''}")

            time.sleep(0.3)

    except Exception as e:
        logging.error(f"Error creating comments for task {task_id}: {e}")


# Load a JSON backup file from disk.
# Centralizing this keeps file reading consistent and makes failures easier to log and diagnose.
def load_backup_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading backup file {file_path}: {e}")
        return None


# Extract the original parent task ID from backup data in a normalized string form.
# This supports both raw IDs and dictionary-style parent references so subtask relationships can be restored reliably.
def get_parent_old_id(task: dict) -> str | None:
    p = task.get("parent")
    if not p:
        return None
    if isinstance(p, dict):
        pid = p.get("id") or p.get("task_id")
        return str(pid) if pid else None
    return str(p)


# Restore tasks for one list while preserving parent-child relationships.
# The function first creates root tasks, then resolves subtasks in additional passes
# so nested task trees can be recreated without requiring pre-sorted input.
def restore_tasks_with_subtasks(list_id, tasks, user_mapping=None, attachments_dir=None):
    id_map: dict[str, str] = {}      # old_task_id -> new_task_id
    pending: list[dict] = []
    created = 0

    # Pass 1: create root tasks first.
    for t in tasks:
        parent_old = get_parent_old_id(t)
        if parent_old:
            pending.append(t)
            continue

        new_id = create_task(list_id, t, user_mapping=user_mapping, attachments_dir=attachments_dir)
        if new_id:
            created += 1
            old_id = t.get("id")
            if old_id:
                id_map[str(old_id)] = new_id

    # Pass 2+: resolve subtasks, including nested ones.
    while pending:
        progress = False
        still_pending = []

        for t in pending:
            parent_old = get_parent_old_id(t)
            parent_new = id_map.get(str(parent_old)) if parent_old else None
            if not parent_new:
                still_pending.append(t)
                continue

            new_id = create_task(
                list_id,
                t,
                user_mapping=user_mapping,
                attachments_dir=attachments_dir,
                parent_task_id=parent_new,
            )
            if new_id:
                created += 1
                old_id = t.get("id")
                if old_id:
                    id_map[str(old_id)] = new_id
                progress = True
            else:
                still_pending.append(t)

        pending = still_pending
        if not progress:
            break

    # Anything left could not be restored because its parent was never created.
    for t in pending:
        logging.warning(
            f"Skipping subtask '{t.get('name')}' (old_id={t.get('id')}) - "
            f"parent not created (old_parent={get_parent_old_id(t)})"
        )

    return created, id_map


# Import one selected backup file into a newly created ClickUp space.
# This function orchestrates the end-to-end restore for a single space:
# it loads the backup, creates the space/folders/lists, prompts for manual statuses, and restores tasks.
def import_single_backup_file(backup_file_path, file_index, total_files, analysis_data=None, user_mapping=None):
    global total_spaces_imported, total_lists_imported, total_tasks_imported

    file_name = os.path.basename(backup_file_path)
    print(f"\n[{file_index + 1}/{total_files}] Processing: {file_name}")

    # Reset local per-file statistics.
    local_spaces = 0
    local_lists = 0
    local_tasks = 0

    try:
        backup_data = load_backup_file(backup_file_path)

        if not backup_data:
            error_msg = f"Failed to load backup file: {file_name}"
            failed_imports.append(error_msg)
            all_import_errors.append(error_msg)
            return False

        space_name = backup_data.get('name', 'Unknown Space')

        # Find the attachments directory.
        backup_dir = os.path.dirname(backup_file_path)
        attachments_dir = os.path.join(backup_dir, "attachments")
        if not os.path.exists(attachments_dir):
            attachments_dir = None
            logging.info(f"No attachments directory found for {file_name}")
        else:
            logging.info(f"Found attachments directory: {attachments_dir}")

        # Get the team ID only once and reuse it for the remaining files.
        if file_index == 0:
            team_id = get_team_id(backup_data)
            # team_id = os.getenv("TEAM_ID_VC")
            if not team_id:
                error_msg = "Failed to get team ID"
                failed_imports.append(error_msg)
                all_import_errors.append(error_msg)
                return False
            import_single_backup_file.team_id = team_id
        else:
            team_id = getattr(import_single_backup_file, 'team_id', None)
            if not team_id:
                team_id = get_team_id(backup_data)
                if not team_id:
                    error_msg = "Failed to get team ID"
                    failed_imports.append(error_msg)
                    all_import_errors.append(error_msg)
                    return False

        # Create a new destination space.
        space_id, new_space_name = create_space(team_id, space_name, file_index + 1)

        if not space_id:
            error_msg = f"Failed to create space for: {space_name}"
            failed_imports.append(error_msg)
            all_import_errors.append(error_msg)
            return False

        print(f"    Created space: {new_space_name}")
        local_spaces += 1

        # Show statuses discovered during analysis so the operator can create them manually.
        custom_statuses = list(analysis_data.get('statuses', [])) if analysis_data else []
        if custom_statuses:
            logging.info(f"This space will need these custom statuses (create manually): {', '.join(custom_statuses[:5])}")
            if len(custom_statuses) > 5:
                logging.info(f"... and {len(custom_statuses) - 5} more statuses")
            print(f"{bcolors.OKBLUE}{bcolors.BOLD}Please, go to your new space (starting with IMP_) and create these statuses.{bcolors.OKGREEN}")
            for stat in custom_statuses:
                print(stat)
            print(f"{bcolors.OKCYAN}that means: {len(custom_statuses)} custom statuses{bcolors.ENDC}")
            while True:
                resp: str = input(f"{bcolors.WARNING}When done, write 'DONE' here: {bcolors.ENDC}\n")
                if resp.strip().upper() == "DONE":
                    break
                else:
                    continue
        else:
            logging.info("No custom statuses detected in this space")

        # Import folders.
        folders = backup_data.get("folders", [])
        if folders:
            print(f"    Importing {len(folders)} folders...")
            for folder_data in folders:
                folder_name = folder_data.get("name", "Unknown_Folder")
                folder_id = create_folder(space_id, folder_name)

                if folder_id:
                    # Import lists inside the folder.
                    for list_data in folder_data.get("lists", []):
                        list_name = list_data.get("name", "Unknown_List")
                        list_id = create_list("folder", folder_id, list_name, custom_statuses)

                        if list_id:
                            local_lists += 1
                            # Import tasks.
                            tasks = list_data.get("tasks", [])
                            print(f"        Importing {len(tasks)} tasks to list: {list_name}")
                            # for task in tasks:
                            #     if create_task(list_id, task, user_mapping, backup_dir):
                            #         local_tasks += 1
                            created_count, _ = restore_tasks_with_subtasks(
                                list_id,
                                tasks,
                                user_mapping=user_mapping,
                                attachments_dir=backup_dir,   # Seems strange but must stay like this.
                            )
                            local_tasks += created_count

        # Import root lists outside folders.
        root_lists = backup_data.get("lists", [])
        if root_lists:
            print(f"    Importing {len(root_lists)} root lists...")
            for list_data in root_lists:
                list_name = list_data.get("name", "Unknown_List")
                list_id = create_list("space", space_id, list_name, custom_statuses)

                if list_id:
                    local_lists += 1
                    # Import tasks.
                    tasks = list_data.get("tasks", [])
                    print(f"        Importing {len(tasks)} tasks to list: {list_name}")
                    # for task in tasks:
                    #     if create_task(list_id, task, user_mapping, backup_dir):
                    #         local_tasks += 1
                    created_count, _ = restore_tasks_with_subtasks(
                        list_id,
                        tasks,
                        user_mapping=user_mapping,
                        attachments_dir=backup_dir,  # Seems strange but must stay like this.
                    )
                    local_tasks += created_count

        # Update cumulative statistics.
        total_spaces_imported += local_spaces
        total_lists_imported += local_lists
        total_tasks_imported += local_tasks

        success_msg = f"{file_name}: Space: {local_spaces}, Lists: {local_lists}, Tasks: {local_tasks}"
        successful_imports.append(success_msg)
        print(f"    Completed: {local_spaces} spaces, {local_lists} lists, {local_tasks} tasks")

        return True

    except Exception as e:
        error_msg = f"{file_name}: {str(e)}"
        failed_imports.append(error_msg)
        all_import_errors.append(error_msg)
        logging.error(f"Critical error importing {file_name}: {e}")
        return False


# Detect whether a directory looks like one backup run directory.
# The heuristic is based on summary files and log files that normally exist in a completed run.
def is_backup_run_dir(p: Path) -> bool:
    if not p.is_dir():
        return False
    if (p / "backup_summary.json").exists():
        return True
    # Also treat it as a run directory if it has the log files shown by the backup layout.
    if any(p.glob("backup_*.log")):
        return True
    if (p / "log_success.txt").exists() or (p / "log_errors.json").exists():
        return True
    return False


# List backup run directories, newest first.
# This supports both passing the parent directory that contains many runs and passing one run directory directly.
def list_backup_runs(backup_root: Union[str, Path]) -> list[Path]:
    backup_root = Path(backup_root)

    if not backup_root.exists():
        return []

    # If the path itself is already a run directory, return it directly.
    if is_backup_run_dir(backup_root):
        return [backup_root]

    runs: list[Path] = []
    for p in backup_root.iterdir():
        if p.name.startswith("."):
            continue
        if (p / ".git").exists():
            continue
        if is_backup_run_dir(p):
            runs.append(p)

    runs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return runs


# Find the main space JSON file in one space directory.
# The function prefers backup_*.json files and ignores backup summary files.
def pick_space_json(space_dir: Union[str, Path]) -> Path | None:
    space_dir = Path(space_dir)
    if not space_dir.is_dir():
        return None

    candidates = []
    for p in space_dir.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() != ".json":
            continue
        if p.name.startswith("backup_summary"):
            continue
        if p.name.startswith("backup_"):
            candidates.append(p)

    if not candidates:
        return None

    # If there are multiple candidates, pick the largest one because it is usually the real space export.
    candidates.sort(key=lambda x: x.stat().st_size, reverse=True)
    return candidates[0]


# Discover all space backups inside one selected backup run.
# Each discovered space includes paths and metadata needed later for user selection and import.
def discover_spaces_in_run(run_dir: Union[str, Path]) -> list[dict]:
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        return []

    spaces: list[dict] = []
    for p in run_dir.iterdir():
        if not p.is_dir():
            continue
        if p.name.startswith("."):
            continue

        # In this layout, p is a space folder such as "Space_..._timestamp".
        json_path = pick_space_json(p)
        if not json_path:
            continue

        space_name = p.name
        space_id = None

        try:
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            space_name = data.get("name") or space_name
            space_id = data.get("id") or None
        except Exception as e:
            logging.warning(f"Cannot parse space json {json_path}: {e}")

        spaces.append({
            "space_dir": p,
            "json_path": json_path,
            "space_name": space_name,
            "space_id": space_id,
        })

    spaces.sort(key=lambda s: (s["space_name"] or "").lower())
    return spaces


# Ask the user to choose an index from a displayed menu.
# The function supports both 1-based input for usability and optional ALL selection for bulk operations.
def choose_index(prompt: str, n: int, *, allow_all: bool = False) -> int | str:
    while True:
        raw = input(prompt).strip()

        if allow_all and raw.upper() in {"ALL", "A", "*"}:
            return "ALL"

        try:
            val = int(raw)
        except ValueError:
            print("Invalid input. Enter a number" + (" or ALL." if allow_all else "."))
            continue

        # 1-based input is the preferred user-facing format.
        if 1 <= val <= n:
            return val - 1

        # Optionally allow 0-based input as a convenience.
        if 0 <= val < n:
            return val

        print(f"Out of range. Choose 1..{n}" + (" or ALL." if allow_all else "."))


# Let the user choose which backup run to import from.
# If only one run exists, the script uses it automatically to reduce unnecessary prompts.
def select_backup_run(backup_root: Union[str, Path]) -> Path | None:
    runs = list_backup_runs(backup_root)
    if not runs:
        print("No backup runs found.")
        return None

    # If there is only one run, skip prompting.
    if len(runs) == 1:
        print(f"\nUsing backup run: {runs[0].name}")
        return runs[0]

    print("\nAvailable backup runs:")
    for i, r in enumerate(runs, start=1):
        print(f"  {i}: {r.name}")

    idx = choose_index("Choose backup run (1..N): ", len(runs), allow_all=False)
    return runs[idx]


# Let the user choose which spaces from the selected run should be restored.
# This supports restoring one space or all discovered spaces from the run.
def select_spaces(spaces: list[dict]) -> list[dict]:
    if not spaces:
        print("No space backups found in this run.")
        return []

    print("\nAvailable spaces in this backup run:")
    for i, s in enumerate(spaces, start=1):
        sid = s.get("space_id") or "?"
        print(f"  {i}: {s['space_name']} (id={sid}) -> {Path(s['json_path']).name}")

    choice = choose_index("Choose space to restore (1..N or ALL): ", len(spaces), allow_all=True)
    if choice == "ALL":
        return spaces
    return [spaces[choice]]


# Merge user information collected from all selected backup files.
# This creates one consolidated user mapping so assignee resolution can work across multiple imported spaces.
def merge_all_users(actual_backups: list) -> dict:
    user_mapping = {}

    for backup_path in actual_backups:
        with open(backup_path, "r", encoding="utf-8") as f:
            space_backup = json.load(f)
        users_in_file = extract_users(space_backup)
        for key, info in users_in_file.items():
            if key not in user_mapping:
                user_mapping[key] = info
            else:
                # Fill missing fields if an earlier record was only partial.
                existing = user_mapping[key]
                if not existing.get("email") and info.get("email"):
                    existing["email"] = info["email"]
                if not existing.get("id") and info.get("id"):
                    existing["id"] = info["id"]
                if not existing.get("username") and info.get("username"):
                    existing["username"] = info["username"]
    return user_mapping


# Main entry point for the enhanced import workflow.
# It coordinates logging, backup selection, pre-import analysis, user mapping,
# batch importing of selected spaces, and final reporting/statistics export.
def main():
    global total_files_processed

    setup_logging()

    print(f"{bcolors.HEADER}ClickUp ENHANCED Import Script{bcolors.ENDC}")
    print("=" * 70)
    print(f"{bcolors.WARNING}SAFETY - this script does not overwrite existing spaces")
    print("Creates new spaces with the 'IMP_' prefix")
    print(f"Custom statuses need to be created manually!{bcolors.ENDC}")

    run_dir = select_backup_run(BACKUP_DIR)
    if not run_dir:
        logging.error("No backup runs found.")
        return 1

    spaces = discover_spaces_in_run(run_dir)
    selected_spaces = select_spaces(spaces)
    if not selected_spaces:
        logging.error("No spaces selected.")
        return 1

    actual_backups = [str(s["json_path"]) for s in selected_spaces]

    print(f"\nSelected for import: {len(actual_backups)} space(s)")
    for s in selected_spaces:
        print(f"  • {s['space_name']} ({Path(s['json_path']).name})")

    # STEP 1: analyze backup data and prepare the structure overview.
    print(f"\n" + "="*70)
    print("STEP 1: ANALYSIS AND STRUCTURE PREPARATION")
    print("="*70)

    analysis_data = comprehensive_backup_analysis(actual_backups)

    # Get current workspace users for later mapping.
    print("\nFetching the list of users in the workspace...")
    with open(actual_backups[0], "r", encoding="utf-8") as f:
        json_content = json.load(f)

    team_id = get_team_id(json_content)
    user_mapping = {}
    if team_id:
        user_mapping = merge_all_users(actual_backups)

    if not user_mapping:
        get_workspace_users(int(team_id))

    if not user_mapping:
        print("There are currently no additional users in the workspace")
    else:
        print(f"Found {len(user_mapping)} users in the workspace")

    # Display the analysis and mapping summary.
    display_analysis_summary(analysis_data, user_mapping)

    # Ask for user confirmation before starting the import.
    print(f"\n" + "="*70)
    print("IMPORT CONFIRMATION")
    print("="*70)

    total_size_mb = 0
    print(f"Found {len(actual_backups)} backup files to import:")
    for i, file_path in enumerate(actual_backups, 1):
        file_name = os.path.basename(file_path)
        try:
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            total_size_mb += size_mb
            print(f"  {i:2d}. {file_name} ({size_mb:.1f} MB)")
        except:
            print(f"  {i:2d}. {file_name}")

    print(f"\nBatch import plan:")
    print(f"   Files: {len(actual_backups)}")
    print(f"   Total size: {total_size_mb:.1f} MB")
    print(f"   Will create: {len(actual_backups)} new spaces")
    print(f"   Estimated time: {len(actual_backups) * 3}-{len(actual_backups) * 8} minutes")
    print(f"   API token: {API_TOKEN[:20]}...")

    # Warnings and operator guidance.
    print(f"\nWARNING:")
    print(f"   • {len(actual_backups)} NEW spaces will be created")
    print(f"   • Custom statuses MUST be created MANUALLY in each list")
    print(f"   • The process may take 30-60 minutes for large files")
    print(f"   • Attachments must be uploaded manually")
    print(f"   • The process can be interrupted with Ctrl+C")

    print(f"\nRECOMMENDED WORKFLOW:")
    print(f"   1. Let the script create all spaces/folders/lists")
    print(f"   2. Then manually add custom statuses to each list in the ClickUp UI")
    print(f"   3. Tasks will import even without statuses - you can adjust them later")

    print(f"\n⏯️  Starting enhanced import of all {len(actual_backups)} files...")
    time.sleep(1)

    # STEP 2: run the actual batch import.
    start_time = time.time()

    print(f"\n" + "="*70)
    print("STEP 2: RUNNING BATCH IMPORT")
    print("="*70)

    # Import each selected backup file.
    for i, backup_file in enumerate(actual_backups):
        try:
            import_single_backup_file(backup_file, i, len(actual_backups), analysis_data, user_mapping)
            total_files_processed += 1

            # Pause between files.
            if i < len(actual_backups) - 1:
                print(f"    Pause {DELAY_BETWEEN_FILES}s before the next file...")
                time.sleep(DELAY_BETWEEN_FILES)

        except KeyboardInterrupt:
            print(f"\nImport interrupted by user on file {i+1}/{len(actual_backups)}")
            break
        except Exception as e:
            logging.error(f"Unexpected error processing file {backup_file}: {e}")
            failed_imports.append(f"{os.path.basename(backup_file)}: Unexpected error")

    end_time = time.time()
    duration = end_time - start_time

    # Final summary.
    print(f"\n" + "="*70)
    print(f"ENHANCED IMPORT COMPLETED!")
    print(f"="*70)
    print(f"Total time: {duration/60:.1f} minutes")
    print(f"Overall statistics:")
    print(f"   Processed files: {total_files_processed}/{len(actual_backups)}")
    print(f"   Created spaces: {total_spaces_imported}")
    print(f"   Created lists: {total_lists_imported}")
    print(f"   Created tasks: {total_tasks_imported}")
    print(f"   Successful imports: {len(successful_imports)}")
    print(f"   Failed imports: {len(failed_imports)}")
    print(f"   Uploaded attachments: {uploaded_attachements}")

    # Successful imports.
    if successful_imports:
        print(f"\nSUCCESSFUL IMPORTS:")
        for success in successful_imports:
            print(f"   • {success}")

    # Failures.
    if failed_imports:
        print(f"\nFAILED IMPORTS:")
        for failure in failed_imports:
            print(f"   • {failure}")

    print(f"\nCheck your test ClickUp account!")
    print(f"Look for {total_spaces_imported} spaces starting with 'IMP_'")

    # Save final statistics to a JSON file.
    try:
        stats_file = os.path.join(SCRIPT_DIR, f"enhanced_import_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump({
                "enhanced_import_summary": {
                    "total_files": len(actual_backups),
                    "processed_files": total_files_processed,
                    "duration_minutes": duration/60,
                    "spaces_imported": total_spaces_imported,
                    "lists_imported": total_lists_imported,
                    "tasks_imported": total_tasks_imported,
                    "successful_imports": len(successful_imports),
                    "failed_imports": len(failed_imports),
                    "uploaded_attachements": uploaded_attachements
                },
                "analysis_data": analysis_data,
                "successful_imports": successful_imports,
                "failed_imports": failed_imports,
                "all_errors": all_import_errors[:50]  # First 50 errors.
            }, f, indent=2, ensure_ascii=False)
        print(f"Detailed statistics saved: {stats_file}")
    except Exception as e:
        logging.error(f"Failed to save stats: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nEnhanced import interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Critical error: {e}")
        sys.exit(1)