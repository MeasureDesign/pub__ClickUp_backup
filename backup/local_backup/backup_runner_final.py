from __future__ import annotations

import subprocess
import sys
import json
from pathlib import Path
import os
import requests
import time
from dotenv import load_dotenv
from datetime import datetime
import random

load_dotenv()

# Find the newest backup folder based on a timestamp embedded in the folder name.
# This is used to identify the most recent backup run directory so the script can
# inspect its manifest or clean it up after the run is complete.
def newest_folder_by_name(base_dir: str | Path) -> Path:
    base: Path = Path(base_dir)

    candidates: list = []
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
        print("No folders matching *_YYYYMMDD_HHMMSS found")
        raise FileNotFoundError("No folders matching *_YYYYMMDD_HHMMSS found")

    return max(candidates, key=lambda x: x[0])[1]

# Run the backup script as a subprocess, collect its output, evaluate whether
# the run succeeded, and send a notification email about the result.
# This function acts as the main coordinator for execution and reporting.
def run_backup_and_notify() -> int | None:
    this_dir: Path = os.path.dirname(os.path.abspath(__file__))
    this_dir = Path(this_dir)

    backup_script_path: Path = Path(this_dir / "backup_final.py")

    p = subprocess.Popen(
        [sys.executable, backup_script_path], 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    pid: int = p.pid
    print(f"Started backup, PID={pid}")

    stdout, stderr = p.communicate()
    rc: int = p.returncode

    if stdout:
        print("=== backup stdout ===")
        print(stdout)

    if stderr:
        print("=== backup stderr ===")
        print(stderr)
    success: bool = (rc == 0)

    try:
        newest_folder: Path = Path(this_dir / newest_folder_by_name(this_dir))
    except FileNotFoundError as e:
        print("Backup folder not found.")
        sys.exit(1)
    manifest_path: Path = Path(newest_folder / "manifest.json")
    manifest = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        success = success and (manifest.get("run_status") == "SUCCESS")

    if success:
        send_mailjet("ClickUp backup SUCCESS", f"The ClickUp backup script SUCCEDED - {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}")
    else:
        send_mailjet("ClickUp backup FAILURE", f"The ClickUp backup script FAILED - {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}")
    return 0 if success else 1

# Send a notification email through the Mailjet API with retry logic for
# transient network and server errors. This helps make alert delivery more
# reliable when short-lived external failures occur.
def send_mailjet(subject: str, text: str, *, max_retries: int = 5, timeout_s: int = 20) -> bool:
    url = "https://api.mailjet.com/v3.1/send"
    load_dotenv()

    mailjet_api_key = os.getenv("MAILJET_API_KEY")
    mailjet_secret_key = os.getenv("MAILJET_SECRET_KEY")
    from_email = os.getenv("MAIL_FROM")
    to_email = os.getenv("MAIL_TO")

    if not all([mailjet_api_key, mailjet_secret_key, from_email, to_email]):
        # Return False if any required Mailjet configuration is missing.
        # The function avoids printing secrets or sensitive configuration details.
        return False

    payload = {
        "Messages": [{
            "From": {"Email": from_email, "Name": "ClickUp Backup"},
            "To": [{"Email": to_email}],
            "Subject": subject,
            "TextPart": text,
        }]
    }

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(
                url,
                auth=(mailjet_api_key, mailjet_secret_key),
                json=payload,
                timeout=timeout_s,
            )

            if 200 <= r.status_code < 300:
                return True

            if r.status_code in (429, 500, 502, 503, 504):
                backoff = min(60, 2 ** (attempt - 1))
                jitter = random.uniform(0, 0.5)
                time.sleep(backoff + jitter)
                continue

            return False

        except requests.RequestException:
            backoff = min(60, 2 ** (attempt - 1))
            jitter = random.uniform(0, 0.5)
            time.sleep(backoff + jitter)

    return False

def main():
    run_backup_and_notify()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error occured: {e}")