from __future__ import annotations

import subprocess
import sys
import json
from pathlib import Path
import os
import requests
import time
from datetime import datetime
import random
import shutil


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
def run_backup_and_notify() -> int:
    this_dir = Path(__file__).resolve().parent
    backup_script_path = this_dir / "backup_cloud.py"

    if not backup_script_path.exists():
        print(f"Backup script not found: {backup_script_path}")
        return 1

    p = subprocess.Popen(
        [sys.executable, str(backup_script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    pid = p.pid
    print(f"Started backup, PID={pid}")

    stdout, stderr = p.communicate()
    rc = p.returncode

    if stdout:
        print("=== backup stdout ===")
        print(stdout)

    if stderr:
        print("=== backup stderr ===")
        print(stderr)

    success = (rc == 0)
    manifest = None

    try:
        newest_folder = this_dir / newest_folder_by_name(this_dir)
    except FileNotFoundError:
        print("Backup folder not found.")
        newest_folder = None

    if newest_folder is not None:
        manifest_path = newest_folder / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                success = success and (manifest.get("run_status") == "SUCCESS")
            except json.JSONDecodeError:
                print(f"Invalid manifest JSON: {manifest_path}")
                success = False
        else:
            print(f"Manifest not found: {manifest_path}")
            success = False
    else:
        success = False

    timestamp_str: str = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    timestamp: datetime = datetime.now()

    if success and (
        (timestamp.year == 2026 and timestamp.month == 3 and timestamp.day == 31) or
        (timestamp.year == 2026 and timestamp.month == 4 and timestamp.day in [1, 2])
    ):
        mail_sent = send_mailjet(
            "ClickUp backup SUCCESS",
            f"The ClickUp backup script SUCCEEDED in production environment - {timestamp_str}"
        )
    else:
        mail_sent = send_mailjet(
            "ClickUp backup FAILURE",
            f"The ClickUp backup script FAILED in production environment - {timestamp_str}"
        )

    if not mail_sent:
        print("Warning: notification email failed to send.")

    return 0 if success else 1


# Send a notification email through the Mailjet API with retry logic for
# transient network and server errors. This helps make alert delivery more
# reliable when short-lived external failures occur.
def send_mailjet(subject: str, text: str, *, max_retries: int = 5, timeout_s: int = 20) -> bool:
    url = "https://api.mailjet.com/v3.1/send"

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


# Remove temporary backup artifacts created during the run, such as the ZIP
# archive and extracted export folder. This helps keep the runtime environment
# clean after the backup has been processed and uploaded.
def cleanup_run_artifacts(export_dir: Path, archive_path: Path | None = None) -> None:
    try:
        if archive_path and archive_path.exists():
            archive_path.unlink()
            print("deleted temp archive")
    except Exception as e:
        print(f"error deleting archive: {e}")

    try:
        if export_dir.exists():
            shutil.rmtree(export_dir)
            print("deleted temp folder")
    except Exception as e:
        print(f"error deleting folder: {e}")


# Program entry point that runs the backup workflow, handles top-level errors,
# and then removes the most recent exported backup artifacts from local storage.
# This function provides a single place for normal script startup and shutdown.
def main():
    try:
        run_backup_and_notify()
    except Exception as e:
        print(f"error: {e}")

    this_dir: Path = Path(os.path.dirname(os.path.abspath(__file__)))
    export_dir: Path = newest_folder_by_name(this_dir)
    zip_path: Path = Path(f"{export_dir}.zip")
    cleanup_run_artifacts(export_dir, zip_path)
    return (0)


# Standard Python entry point for running the script directly.
if __name__ == "__main__":
    raise SystemExit(main())