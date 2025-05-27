import functions_framework
import csv
import os
from google.cloud import storage
from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# メール送信用関数
def send_email(subject, content):
    message = Mail(
        from_email=os.environ.get("FROM_EMAIL"),
        to_emails=os.environ.get("TO_EMAIL"),
        subject=subject,
        plain_text_content=content,
    )
    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        sg.send(message)
        print("📧 Email sent.")
    except Exception as e:
        print(f"SendGrid error: {e}")

# メイン処理（Cloud Storageトリガー）
@functions_framework.cloud_event
def process_csv(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"🔔 受信イベント - バケット: {bucket_name}, ファイル名: {file_name}")

    # Storage クライアント
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # 一時ファイルにダウンロード
    temp_file_path = f"/tmp/{file_name}"
    blob.download_to_filename(temp_file_path)
    print(f"📥 一時保存: {temp_file_path}")

    # BigQuery クライアント
    bq_client = bigquery.Client()
    dataset_id = "exam_dataset"
    table_id = "csv_test"
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # CSV を読み込み BigQuery 用データに変換
    rows_to_insert = []
    with open(temp_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # 空のフィールドがある行はスキップ（必要に応じて調整）
            if any(v.strip() == "" for v in row.values()):
                print(f"⚠️ 空の値が含まれている行をスキップ: {row}")
                continue
            rows_to_insert.append(row)

    # BigQuery に挿入
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"❌ BigQuery挿入エラー: {errors}")
    else:
        print(f"✅ BigQueryに {len(rows_to_insert)} 件を書き込み完了。")
        send_email(
            subject="CSVアップロード成功通知",
            content=f"{file_name} に含まれる {len(rows_to_insert)} 件のデータを BigQuery に保存しました。"
        )