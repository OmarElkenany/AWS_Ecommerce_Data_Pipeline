import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

# تحميل ملف .env من نفس مجلد المشروع تلقائيًا
load_dotenv(override=True)

# Configuration
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "api_output")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "samsung-aws-task")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/orders")

# Create S3 client
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def upload_directory_to_s3(local_directory, bucket_name, s3_prefix):
    local_directory = Path(local_directory)

    if not local_directory.exists():
        raise FileNotFoundError(f"Directory not found: {local_directory}")

    files = [file for file in local_directory.rglob("*.json") if file.is_file()]

    if not files:
        print(f"No JSON files found in '{local_directory}'.")
        return

    print(f"Found {len(files)} JSON files in '{local_directory}'.\n")

    for file_path in files:
        file_name = file_path.name
        
        # المسار المباشر على S3 بدون تقسيم تواريخ
        s3_key = f"{s3_prefix}/{file_name}"

        print(f"Uploading: {file_name} -> s3://{bucket_name}/{s3_key}")

        try:
            s3.upload_file(str(file_path), bucket_name, s3_key)
            print("Uploaded successfully.\n")
        except Exception as e:
            print(f"Failed to upload {file_name}: {e}\n")

if __name__ == "__main__":
    upload_directory_to_s3(OUTPUT_DIR, BUCKET_NAME, S3_PREFIX)
    print("================================")
    print("All JSON files uploaded successfully")
    print("================================")