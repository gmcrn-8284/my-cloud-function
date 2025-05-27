# My Cloud Function

This is a Google Cloud Function that handles uploaded CSV files,
parses them, stores data in BigQuery, and sends a notification email.

## Requirements
- Python 3.11
- See `requirements.txt` for dependencies

## Deployment
```bash
gcloud functions deploy my-function-name \
  --runtime python311 \
  --entry-point main \
  --trigger-bucket my-upload-bucket \
  --env-vars-file .env.yaml \
  --gen2
