# ClickUp Backup and Notification Scripts

NOTE: This version of the code is meant tobe executed locally.

## Overview

This project contains two Python scripts that work together to perform automated ClickUp backups (presumably via cron), apply retention rules, and send email notifications about the result.

The first script, `backup_final.py`, performs the actual backup process. It connects to the ClickUp API, exports spaces, folders, lists, tasks, comments, and attachments, writes the backup to a local export directory and enforces retention rules for stored backups.

The second script is a wrapper script that executes `backup_final.py` as a subprocess, checks whether the backup completed successfully and sends a notification email using Mailjet.

Together, these scripts provide a complete backup workflow suitable for scheduled execution.

---

## How It Works

### 1. Backup script (`backup_cloud.py`)

The backup script is responsible for the full ClickUp export workflow.

It performs the following steps:

- Loads configuration from environment variables
- Connects to the ClickUp API
- Discovers available teams and spaces
- Exports spaces, folders, lists, tasks, comments, and attachments
- Writes backup data to a timestamped export directory
- Generates summary files and a manifest
- Applies retention rules

A run is considered successful only if the export completes correctly and the final `manifest.json` contains:

```json
"run_status": "SUCCESS"
```

### 2. Wrapper / notification script

The second script acts as the execution and notification layer.

It performs the following steps:
- Locates backup_cloud.py in the same directory
- Starts it as a subprocess
- Captures standard output and standard error
- Checks the subprocess return code
- Locates the newest backup folder
- Reads manifest.json
- Confirms that run_status is SUCCESS
- Sends a success or failure email via Mailjet

This design provides an extra validation layer beyond the subprocess exit code, because the backup is only treated as successful if both the process exits cleanly and the manifest confirms a successful run.

### Script Relationship

The scripts are intended to be used together in this order:
- The wrapper script starts backup_final.py
- backup_final.py performs the backup and writes output locally
- The wrapper script validates the result using the newest backup folder and its manifest
- The wrapper script sends an email notification

### Requirements
- Python 3.10 or newer recommended
- Access to the ClickUp API

Dependencies include:
- requests
- tqdm

Install dependencies with:
```
pip install -r requirements.txt 
```

### Environment Variables

Both scripts rely on environment variables for configuration.

#### ClickUp
- API_TOKEN_MAIN
ClickUp API token used for backup access

#### Mailjet
- MAILJET_API_KEY
Mailjet API key
- MAILJET_SECRET_KEY
Mailjet secret key
- MAIL_FROM
Sender email address
- MAIL_TO
Recipient email address for notifications

### Output Structure
During execution, backup_cloud.py creates a timestamped local export directory similar to:
```
ClickUp_Backup_Complete_YYYYMMDD_HHMMSS/
```

On the first day of the month, the directory name may include a month prefix.

Inside the export directory, the script writes files such as:
```
backup_summary.json
log_success.txt
log_errors.json
manifest.json
backup_<space_name>_<space_id>_<timestamp>.json
attachments/
```

### Retention Logic
The backup script applies retention rules locally.

#### Daily backups
It keeps:
- the most recent successful daily backups (3)
- a limited number of recent non-successful daily backups

#### Monthly backups
It keeps:
- the newest successful backup for each month
- if no successful backup exists for a given month, the newest backup for that month

This allows short-term operational recovery through daily backups and longer-term recovery through monthly snapshots.

### Success and Failure Rules
The wrapper script treats the backup as successful only when both conditions are met:
1) backup_cloud.py exits with return code 0
2) The newest backup folder contains a valid manifest.json with:
```json
"run_status": "SUCCESS"
```
If either condition fails, the wrapper sends a failure email

### Notes
* The wrapper script expects backup_final.py to exist in the same directory.
* Email notifications depend on Mailjet credentials being correctly configured.
* A clean subprocess exit alone is not enough for success; the manifest status is also checked.