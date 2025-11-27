"""Boto3 utilities for S3-compatible storage operations."""

import io
import os
import shutil
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from ..modules.logger import log


def extract_region_from_url(endpoint_url: str) -> Optional[str]:
    """Extract the region from an S3-compatible endpoint URL.

    Args:
        endpoint_url: The S3 endpoint URL.

    Returns:
        The extracted region or None if not found.
    """
    parsed_url = urlparse(endpoint_url)

    # AWS/Backblaze S3-like URL (e.g., s3.us-west-2.amazonaws.com)
    if ".s3." in endpoint_url:
        return endpoint_url.split(".s3.")[1].split(".")[0]

    # DigitalOcean Spaces-like URL (e.g., nyc3.digitaloceanspaces.com)
    if parsed_url.netloc.endswith(".digitaloceanspaces.com"):
        return endpoint_url.split(".")[1].split(".digitaloceanspaces.com")[0]

    return None


def create_boto_client(
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    region: Optional[str] = None,
) -> Tuple[Any, Any]:
    """Create a boto3 S3 client and transfer config.

    Args:
        endpoint_url: The S3 endpoint URL.
        access_key_id: AWS access key ID.
        secret_access_key: AWS secret access key.
        region: AWS region (auto-detected if not provided).

    Returns:
        Tuple of (boto_client, transfer_config).
    """
    from boto3 import session
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config

    # Auto-detect region if not provided
    if region is None:
        region = extract_region_from_url(endpoint_url)

    bucket_session = session.Session()

    boto_config = Config(
        signature_version="s3v4",
        retries={"max_attempts": 3, "mode": "standard"},
        tcp_keepalive=True,
    )

    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,  # 8MB - start multipart for files > 8MB
        max_concurrency=8,  # 8 concurrent upload threads per file
        multipart_chunksize=8 * 1024 * 1024,  # 8MB chunks
        use_threads=True,
    )

    boto_client = bucket_session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=boto_config,
        region_name=region,
    )

    return boto_client, transfer_config


def upload_file_to_bucket(
    file_location: str,
    file_name: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str,
    prefix: Optional[str] = None,
    extra_args: Optional[Dict[str, Any]] = None,
    region: Optional[str] = None,
    presigned_url_expiry: int = 604800,
    progress_callback: Optional[callable] = None,
) -> str:
    """Upload a file to S3-compatible storage and return a presigned URL.

    Args:
        file_location: Local file path to upload.
        file_name: Name of the file in the bucket.
        endpoint_url: The S3 endpoint URL.
        access_key_id: AWS access key ID.
        secret_access_key: AWS secret access key.
        bucket_name: S3 bucket name.
        prefix: Optional prefix (folder) in the bucket.
        extra_args: Extra arguments for S3 upload (e.g., ContentType, ACL).
        region: AWS region (auto-detected if not provided).
        presigned_url_expiry: Presigned URL expiry in seconds (default: 7 days).
        progress_callback: Optional callback for upload progress (receives bytes).

    Returns:
        Presigned URL for the uploaded file.

    Example:
        url = upload_file_to_bucket(
            file_location="/path/to/file.jpg",
            file_name="output.jpg",
            endpoint_url="https://s3.us-west-2.amazonaws.com",
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            bucket_name="my-bucket",
            extra_args={"ContentType": "image/jpeg"},
        )
    """
    boto_client, transfer_config = create_boto_client(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
    )

    key = f"{prefix}/{file_name}" if prefix else file_name

    log.debug(f"Uploading {file_location} to s3://{bucket_name}/{key}")

    upload_file_args = {
        "Filename": file_location,
        "Bucket": bucket_name,
        "Key": key,
        "Config": transfer_config,
    }

    if extra_args:
        upload_file_args["ExtraArgs"] = extra_args

    if progress_callback:
        upload_file_args["Callback"] = progress_callback

    boto_client.upload_file(**upload_file_args)

    presigned_url = boto_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=presigned_url_expiry,
    )

    log.info(f"File uploaded successfully: {presigned_url}")

    return presigned_url


def upload_file_to_bucket_or_local(
    file_location: str,
    file_name: str,
    bucket_name: str,
    endpoint_url: Optional[str] = None,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    prefix: Optional[str] = None,
    extra_args: Optional[Dict[str, Any]] = None,
    region: Optional[str] = None,
    presigned_url_expiry: int = 604800,
    progress_callback: Optional[callable] = None,
    local_fallback_dir: str = "local_upload",
) -> str:
    """Upload a file to S3-compatible storage or save locally as fallback.

    If S3 credentials are not provided, the file is saved to a local directory.

    Args:
        file_location: Local file path to upload.
        file_name: Name of the file in the bucket.
        bucket_name: S3 bucket name.
        endpoint_url: The S3 endpoint URL (optional, triggers local fallback if None).
        access_key_id: AWS access key ID (optional).
        secret_access_key: AWS secret access key (optional).
        prefix: Optional prefix (folder) in the bucket.
        extra_args: Extra arguments for S3 upload.
        region: AWS region (auto-detected if not provided).
        presigned_url_expiry: Presigned URL expiry in seconds (default: 7 days).
        progress_callback: Optional callback for upload progress.
        local_fallback_dir: Directory for local fallback storage.

    Returns:
        Presigned URL for the uploaded file or local file path.
    """
    if not endpoint_url or not access_key_id or not secret_access_key:
        log.warn("S3 credentials not provided, saving to local fallback directory")
        os.makedirs(local_fallback_dir, exist_ok=True)
        local_upload_location = os.path.join(local_fallback_dir, file_name)
        shutil.copyfile(file_location, local_upload_location)
        log.info(f"File saved locally: {local_upload_location}")
        return local_upload_location

    return upload_file_to_bucket(
        file_location=file_location,
        file_name=file_name,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket_name=bucket_name,
        prefix=prefix,
        extra_args=extra_args,
        region=region,
        presigned_url_expiry=presigned_url_expiry,
        progress_callback=progress_callback,
    )


def upload_bytes_to_bucket(
    file_data: bytes,
    file_name: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str,
    prefix: Optional[str] = None,
    extra_args: Optional[Dict[str, Any]] = None,
    region: Optional[str] = None,
    presigned_url_expiry: int = 604800,
) -> str:
    """Upload in-memory bytes to S3-compatible storage and return a presigned URL.

    Args:
        file_data: Bytes data to upload.
        file_name: Name of the file in the bucket.
        endpoint_url: The S3 endpoint URL.
        access_key_id: AWS access key ID.
        secret_access_key: AWS secret access key.
        bucket_name: S3 bucket name.
        prefix: Optional prefix (folder) in the bucket.
        extra_args: Extra arguments for S3 upload (e.g., ContentType, ACL).
        region: AWS region (auto-detected if not provided).
        presigned_url_expiry: Presigned URL expiry in seconds (default: 7 days).

    Returns:
        Presigned URL for the uploaded file.

    Example:
        url = upload_bytes_to_bucket(
            file_data=b"Hello, World!",
            file_name="hello.txt",
            endpoint_url="https://s3.us-west-2.amazonaws.com",
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            bucket_name="my-bucket",
            extra_args={"ContentType": "text/plain"},
        )
    """
    boto_client, transfer_config = create_boto_client(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
    )

    key = f"{prefix}/{file_name}" if prefix else file_name

    log.debug(f"Uploading bytes to s3://{bucket_name}/{key}")

    file_size = len(file_data)

    # For small files (<8MB), use put_object directly - faster, no multipart overhead
    if file_size < 8 * 1024 * 1024:
        put_args = {"Body": file_data}
        if extra_args:
            put_args.update(extra_args)

        boto_client.put_object(
            Bucket=bucket_name,
            Key=key,
            **put_args,
        )
    else:
        # Use upload_fileobj with multipart for larger files
        upload_file_args = {
            "Bucket": bucket_name,
            "Key": key,
            "Config": transfer_config,
        }
        if extra_args:
            upload_file_args["ExtraArgs"] = extra_args

        boto_client.upload_fileobj(
            io.BytesIO(file_data),
            **upload_file_args,
        )

    presigned_url = boto_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=presigned_url_expiry,
    )

    log.info(f"Bytes uploaded successfully: {presigned_url}")

    return presigned_url


def download_file_from_bucket(
    file_name: str,
    destination_path: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str,
    prefix: Optional[str] = None,
    region: Optional[str] = None,
    progress_callback: Optional[callable] = None,
) -> str:
    """Download a file from S3-compatible storage.

    Args:
        file_name: Name of the file in the bucket.
        destination_path: Local path to save the downloaded file.
        endpoint_url: The S3 endpoint URL.
        access_key_id: AWS access key ID.
        secret_access_key: AWS secret access key.
        bucket_name: S3 bucket name.
        prefix: Optional prefix (folder) in the bucket.
        region: AWS region (auto-detected if not provided).
        progress_callback: Optional callback for download progress.

    Returns:
        The destination path where the file was saved.
    """
    boto_client, transfer_config = create_boto_client(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
    )

    key = f"{prefix}/{file_name}" if prefix else file_name

    log.debug(f"Downloading s3://{bucket_name}/{key} to {destination_path}")

    download_args = {
        "Bucket": bucket_name,
        "Key": key,
        "Filename": destination_path,
        "Config": transfer_config,
    }

    if progress_callback:
        download_args["Callback"] = progress_callback

    boto_client.download_file(**download_args)

    log.info(f"File downloaded successfully to {destination_path}")

    return destination_path
