import logging
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from botocore.exceptions import ClientError

from payload_migration.uploader.hcp_uploader import HcpUploader
from mypy_boto3_s3.service_resource import S3ServiceResource

logger = logging.getLogger(__name__)

class S3UploadError(Exception):
    pass

class HcpUploaderImpl(HcpUploader):
    def __init__(self, s3: S3ServiceResource):
        self._s3 = s3
        
        
    def upload_file(
        self, 
        file_path: Path,
        bucket: str, 
        object_name: str
    ) -> None:
        try:
            # s3 = boto3.resource('s3')
            self._s3.Bucket(bucket).upload_file(str(file_path), object_name)
        except ClientError as e:
            logging.error(f"Failed to upload {file_path}: {e}")
            raise S3UploadError(f"Failed to upload {file_path}") from e

    def upload_files(
        self,
        file_paths: list[Path], 
        bucket: str, 
        max_workers: int
    ) -> None:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                # TODO change object name
                executor.submit(self.upload_file, path, bucket, path.name) 
                for path in file_paths
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
        paths: list[tuple[Path, Path]] = [
            (f, f.relative_to(directory))
            for f in directory.rglob('*') 
            if f.is_file()
        ]
        
        for absolute_path, relative_path, f2 in paths:
            self.upload_file(absolute_path, 'foo', relative_path)
            
# TODO:
# - dokonczyc i sprawdzic obecna logike
# - czy upload_files mozna napisac zgrabniej
# - przetestowac czy tworzy subfoldery tak jak chce  
# - dorobic nie sprawdzanie certyfikatu SSL
