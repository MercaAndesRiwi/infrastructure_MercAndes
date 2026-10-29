"""S3 client for uploading F7 promotion files and historical F1 datasets."""

from __future__ import annotations

import os
from datetime import datetime

import boto3
from botocore.client import Config


def _s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
    )


def upload_promotion_file(contents: bytes, filename: str) -> str:
    """
    Upload a weekly promotion file (F7) to S3 under:
    bronze/f7_promotions/YYYY/MM/DD/<filename>
    Returns the full s3_key.
    """
    s3 = _s3_client()
    bucket = os.getenv("S3_DATA_LAKE_BUCKET_NAME", "mercaandes-data-lake")
    bronze_folder = os.getenv("S3_BRONZE_FOLDER", "bronze")
    f7_folder = os.getenv("S3_F7_PROMOTIONS_FOLDER", "f7_promotions")

    today = datetime.utcnow()
    key = (
        f"{bronze_folder}/{f7_folder}/"
        f"{today.year:04d}/{today.month:02d}/{today.day:02d}/"
        f"{filename}"
    )

    s3.put_object(Bucket=bucket, Key=key, Body=contents)
    return key


def upload_historical_file(contents: bytes, source: str, filename: str) -> str:
    """
    Upload a historical file (e.g. Kaggle F1) to S3 under:
    bronze/<source>/<filename>
    Returns the full s3_key.
    """
    s3 = _s3_client()
    bucket = os.getenv("S3_DATA_LAKE_BUCKET_NAME", "mercaandes-data-lake")
    bronze_folder = os.getenv("S3_BRONZE_FOLDER", "bronze")

    key = f"{bronze_folder}/{source}/{filename}"

    s3.put_object(Bucket=bucket, Key=key, Body=contents)
    return key
