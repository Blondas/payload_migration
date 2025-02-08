import logging
import subprocess
from pathlib import Path
from typing import Optional

from payload_migration.uploader.hcp_uploader import HcpUploader

logger = logging.getLogger(__name__)


class CliS3UploadError(Exception):
    pass


class HcpUploaderAwsCliImpl(HcpUploader):
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str, 
        verify_ssl: bool
    ):
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._verify_ssl = verify_ssl

    def upload_dir(
        self,
        directory: Path,
        extra_args: Optional[list[str]] = None
    ) -> None:
        source_path = str(directory)
        if not source_path.endswith('/'):
            source_path += '/'

        destination = f"s3://{self._s3_bucket}/{self._s3_prefix}"
        if not destination.endswith('/'):
            destination += '/'

        cmd = [
            "aws", "s3", "cp",
            source_path,
            destination,
            "--recursive",
            "--quiet",  
            "--only-show-errors"
        ]

        if self._verify_ssl:
            cmd.extend("--no-verify-ssl")

        try:
            logger.info(f"Starting upload from {source_path} to {destination}, command executed: {' '.join(cmd)}")

            # Run the AWS CLI command
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )

            if result.stderr:
                logger.warning(f"Upload completed with warnings: {result.stderr}")

            logger.info("Upload completed successfully")

        except subprocess.CalledProcessError as e:
            error_msg = f"Upload failed: {e.stderr}"
            logger.error(error_msg)
            raise CliS3UploadError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during upload: {str(e)}"
            logger.error(error_msg)
            raise CliS3UploadError(error_msg) from e