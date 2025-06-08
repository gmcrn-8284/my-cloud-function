import functions_framework
import csv
import os
from google.cloud import storage
from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime, timezone

# メール送信用関数（宛先と本文を引数で受け取るよう修正）
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

# Cloud Function 本体
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

    # CSV 読み込みとデータ整形
    rows_to_insert = []
    email_content_map = {}

    with open(temp_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # 空のフィールドがある行をスキップ
            if any(v.strip() == "" for v in row.values()):
                print(f"⚠️ 空の値が含まれている行をスキップ: {row}")
                continue

            # create_at を現在時刻で追加（UTC）
            row["create_at"] = datetime.now(timezone.utc).isoformat()

            rows_to_insert.append(row)

            # 送信条件とメール本文を格納
            send_flg = row.get("send_flg", "").strip()
            email = row.get("email", "").strip()
            content = row.get("content", "").strip()
            if email and (send_flg == "1" or send_flg == "") and content:
                email_content_map[email] = content

    # BigQuery に保存
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"❌ BigQuery挿入エラー: {errors}")
    else:
        print(f"✅ BigQueryに {len(rows_to_insert)} 件を書き込み完了。")

        # メール送信（個別コンテンツ付き）
        for email, content in email_content_map.items():
            send_email(
                to_email=email,
                subject="CSVアップロード完了通知",
                content=content
            )
