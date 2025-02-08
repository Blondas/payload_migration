import logging
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class CliS3UploadError(Exception):
    pass


class HcpUploaderAwsCliImpl:
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str
    ):
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix

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
            "--recursive",
            "--quiet",  
            "--follow-symlinks",
            "--metadata-directive", "COPY",
            "--only-show-errors",  
            source_path,
            destination
        ]

        # Add any extra arguments
        if extra_args:
            cmd.extend(extra_args)

        try:
            logger.info(f"Starting upload from {source_path} to {destination}")

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

    @staticmethod
    def get_upload_command(
        source_dir: Path,
        s3_bucket: str,
        s3_prefix: str,
        extra_args: Optional[list[str]] = None
    ) -> list[str]:
        """
        Get the AWS CLI command that would be run (useful for debugging or manual execution)
        """
        source_path = str(source_dir)
        if not source_path.endswith('/'):
            source_path += '/'

        destination = f"s3://{s3_bucket}/{s3_prefix}"
        if not destination.endswith('/'):
            destination += '/'

        cmd = [
            "aws", "s3", "cp",
            "--recursive",
            "--quiet",
            "--follow-symlinks",
            "--metadata-directive", "COPY",
            "--only-show-errors",
            source_path,
            destination
        ]

        if extra_args:
            cmd.extend(extra_args)

        return cmd