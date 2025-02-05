import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from botocore.exceptions import ClientError

from payload_migration.uploader.upload_target import UploadTarget
from payload_migration.uploader.hcp_uploader import HcpUploader
from mypy_boto3_s3.service_resource import S3ServiceResource

logger = logging.getLogger(__name__)

class S3UploadError(Exception):
    pass

class HcpUploaderImpl(HcpUploader):
    def __init__(
        self, 
        s3: S3ServiceResource,
        max_workers: int,
        s3_bucket: str, 
        s3_prefix: str
    ):
        self._s3 = s3
        self._max_workers = max_workers
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        
         
    def _upload_file(
        self,
        file_path: Path,
        object_name: str
    ) -> None:
        try:
            self._s3.Bucket(self._s3_bucket).upload_file(str(file_path), object_name)
        except ClientError as e:
            error_msg = f"Failed to upload {file_path} to {self._s3_bucket}/{object_name}: {str(e)}"
            logging.error(error_msg)
            raise S3UploadError(error_msg) from e

    def _upload_files(
        self,
        upload_targets: list[UploadTarget]
    ) -> None:
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = [
                # TODO change object name
                executor.submit(self._upload_file, target.local_path, target.s3_key) 
                for target in upload_targets
            ]
            
            errors = []
            for future in as_completed(futures):
                try:
                    future.result()
                except S3UploadError as e:
                    errors.append(e)
                    continue

        if errors:
            logger.error(f"Failed uploads: {len(errors)} files")
            
    def upload_dir(
        self,
        directory: Path
    ) -> None:
        upload_targets: list[UploadTarget] = [
            UploadTarget(
                local_path=f,
                # s3_key=str(f.relative_to(directory))
                s3_key=str(Path(self._s3_prefix) / f.relative_to(directory))
            )
            for f in directory.rglob('*') 
            if f.is_file()
        ]
        
        self._upload_files(upload_targets)
