import functions_framework
import csv
import os
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# メール送信用関数
def send_email(to_email, subject, content):
    message = Mail(
        from_email=os.environ.get("FROM_EMAIL"),
        to_emails=to_email,
        subject=subject,
        plain_text_content=content,
    )
    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        sg.send(message)
        print(f"📧 Email sent to {to_email}")
    except Exception as e:
        print(f"SendGrid error for {to_email}: {e}")

# Cloud Function エントリーポイント
@functions_framework.cloud_event
def process_csv(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"🔔 受信イベント - バケット: {bucket_name}, ファイル名: {file_name}")

    # Cloud Storage からファイルをダウンロード
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    temp_file_path = f"/tmp/{file_name}"
    blob.download_to_filename(temp_file_path)
    print(f"📥 一時保存: {temp_file_path}")

    # BigQuery クライアント
    bq_client = bigquery.Client()
    dataset_id = "exam_dataset"
    table_id = "csv_test"
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # 現在のタイムスタンプ
    current_time = datetime.utcnow().isoformat()

    # CSV 読み込みとデータ整形
    rows_to_insert = []
    recipients = []

    with open(temp_file_path, mode='r', encoding='utf-8', errors='replace') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # 空のフィールドがある行をスキップ
            if any(v.strip() == "" for v in row.values()):
                print(f"⚠️ 空の値が含まれている行をスキップ: {row}")
                continue

            # create_at を現在の時刻で追加（CSVの値があっても上書き）
            row["create_at"] = current_time
            rows_to_insert.append(row)

            # メール送信対象の抽出
            send_flg = row.get("send_flg", "").strip()
            email = row.get("email", "").strip()
            if email and (send_flg == "1" or send_flg == ""):
                recipients.append(email)

    # BigQuery に保存
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"❌ BigQuery挿入エラー: {errors}")
    else:
        print(f"✅ BigQueryに {len(rows_to_insert)} 件を書き込み完了。")

        # メール送信
        for email in recipients:
            send_email(
                to_email=email,
                subject="CSVアップロード完了通知",
                content=f"{file_name} に含まれるデータを BigQuery に保存しました。"
            )
