# ClickUp Backup and Notification Scripts

NOTE: This version of the code is not meant to be directly executable locally because of how the google cloud lib and environment work.

## Overview

This project contains two Python scripts that work together to perform automated ClickUp backups, upload successful backups to Google Cloud Storage, apply retention rules, and send email notifications about the result.

The first script, `backup_cloud.py`, performs the actual backup process. It connects to the ClickUp API, exports spaces, folders, lists, tasks, comments, and attachments, writes the backup to a local export directory, creates a ZIP archive, uploads the archive and manifest to Google Cloud Storage, and enforces retention rules for stored backups.

The second script is a wrapper script that executes `backup_cloud.py` as a subprocess, checks whether the backup completed successfully, sends a notification email using Mailjet, and then removes temporary local backup artifacts.

Together, these scripts provide a complete backup workflow suitable for scheduled production execution.

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
- Creates a ZIP archive of the backup directory
- Uploads `backup.zip` and `manifest.json` to Google Cloud Storage
- Applies retention rules in the GCS bucket

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
- Removes the local backup folder and ZIP archive after the run

This design provides an extra validation layer beyond the subprocess exit code, because the backup is only treated as successful if both the process exits cleanly and the manifest confirms a successful run.

### Script Relationship

The scripts are intended to be used together in this order:
- The wrapper script starts backup_cloud.py
- backup_cloud.py performs the backup and writes output locally
- backup_cloud.py uploads successful artifacts to Google Cloud Storage
- The wrapper script validates the result using the newest backup folder and its manifest
- The wrapper script sends an email notification
- The wrapper script deletes local temporary artifacts

This means the local backup directory is treated as a temporary working area, while Google Cloud Storage serves as the persistent backup destination.

### Requirements
- Python 3.10 or newer recommended
- Access to the ClickUp API
- A Google Cloud project with:
    - Cloud Storage bucket
    - Service account credentials available in the runtime environment
    - A Mailjet account for email notifications
    - Required Python packages installed

Dependencies include:
- requests
- tqdm
- google-cloud-storage

Install dependencies with:
```
pip install -r requirements.txt 
```

### Environment Variables

Both scripts rely on environment variables for configuration.

#### ClickUp
- API_TOKEN_MAIN
ClickUp API token used for backup access

#### Google Cloud
- GCLOUD_PROJECT
Google Cloud project ID
- BUCKET_NAME
Google Cloud Storage bucket name where backups are uploaded

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

It also creates a ZIP archive of the directory:
```
ClickUp_Backup_Complete_YYYYMMDD_HHMMSS.zip
```

For successful runs, the archive and manifest are uploaded to Google Cloud Storage under either:
```
daily/ClickUp_Backup_Complete_YYYYMMDD_HHMMSS/
```

or

```
monthly/ClickUp_Backup_Complete_YYYYMMDD_HHMMSS/
```

depending on whether the run took place on the first day of the month.

### Retention Logic

The backup script applies retention rules in Google Cloud Storage.

#### Daily backups
It keeps:
- the most recent successful daily backups
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

## Deployment to google cloud
* Ideally deployed as cloud run job as the execution takes some time
* Build the image locally:
```
docker build -t image_name:flag .
```
* Create Artifact repository on google cloud:
```
gcloud artifacts repositories create repo_name \
  --repository-format=docker \
  --location=your_server_location
```
* Tag the image for the purposes of pushing to google cloud artifact repository:
```
docker tag image_name:image_tag \
  your_server_location-docker.pkg.dev/your_project_id/repo_name/image_name:image_flag
```
* Configure authentication:
```
gcloud auth configure-docker your_server_location-docker.pkg.dev
```
* Push to the artifact repository:
```
docker push your_server_location-docker.pkg.dev/your_project_id/your_repo_name/image_name:image_flag
```

Then you can find your image when creating a cloud run job.

The job requires a storage bucket to be created and it's name in the environment variables of the job.

When creating the job, and meaning to run it cron-like via the scheduler, the service account of the job needs these roles:
* Cloud Run Invoker
* Cloud Storage Admin


### Notes
* The wrapper script expects backup_cloud.py to exist in the same directory.
* The local backup directory is temporary and may be deleted by the wrapper after execution.
* Persistent backup storage is expected to be Google Cloud Storage.
* Email notifications depend on Mailjet credentials being correctly configured.
* A clean subprocess exit alone is not enough for success; the manifest status is also checked.