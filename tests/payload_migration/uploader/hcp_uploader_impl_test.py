from pathlib import Path
from typing import Any, Generator
import pytest
from unittest.mock import Mock, MagicMock
from botocore.exceptions import ClientError

from payload_migration.uploader.hcp_uploader_impl import HcpUploaderImpl, S3UploadError
from payload_migration.uploader.upload_target import UploadTarget


@pytest.fixture
def mock_s3_resource() -> MagicMock:
    mock_s3 = MagicMock()
    mock_s3.Bucket.return_value = MagicMock()
    return mock_s3


@pytest.fixture
def uploader(mock_s3_resource: Mock) -> HcpUploaderImpl:
    return HcpUploaderImpl(s3=mock_s3_resource, max_workers=2, bucket='test-bucket')


@pytest.fixture
def temp_dir(tmp_path: Path) -> Generator[Path, None, None]:
    base_dir = tmp_path / "base_dir"
    base_dir.mkdir()
    
    subdir_1 = base_dir / "subdir_1"
    subdir_1.mkdir(parents=True, exist_ok=True)
    
    subdir_2 = base_dir / "subdir_2a" / "subdir_2b"
    subdir_2.mkdir(parents=True, exist_ok=True)
    
    (subdir_1 / "file1_1").write_text("file1_1")
    (subdir_1 / "file1_2").write_text("file1_2")
    (subdir_2 / "file2_1").write_text("file2_1")
    (subdir_2 / "file2_2").write_text("file2_2")

    yield base_dir

class TestHcpUploaderImpl:
    def test_upload_file_success(
        self, 
        uploader: HcpUploaderImpl, 
        mock_s3_resource: Mock
    ) -> None:
        file_path = Path("test.txt")
        bucket = "test-bucket"
        object_name = "test.txt"
    
        uploader._upload_file(file_path, object_name)
    
        mock_s3_resource.Bucket.assert_called_once_with(bucket)
        mock_s3_resource.Bucket.return_value.upload_file.assert_called_once_with(
            str(file_path), object_name
        )
    
    
    def test_upload_file_failure(
        self, 
        uploader: HcpUploaderImpl, 
        mock_s3_resource: Mock
    ) -> None:
        file_path = Path("test.txt")
        bucket = "test-bucket"
        object_name = "test.txt"

        client_error = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "The bucket does not exist"}},
            "PutObject"
        )
        mock_s3_resource.Bucket.return_value.upload_file.side_effect = client_error

        # Act & Assert
        with pytest.raises(S3UploadError) as exc_info:
            uploader._upload_file(file_path, object_name)

        expected_error_msg = f"Failed to upload {file_path} to {bucket}/{object_name}: {str(client_error)}"
        assert str(exc_info.value) == expected_error_msg
        assert exc_info.value.__cause__ == client_error  # Verify the exception chain
    
    
    def test_upload_files_success(
        self, 
        uploader: HcpUploaderImpl, 
        mock_s3_resource: Mock
    ) -> None:
        upload_targets = [
            UploadTarget(local_path=Path("file1.txt"), s3_key="file1.txt"),
            UploadTarget(local_path=Path("file2.txt"), s3_key="file2.txt")
        ]
    
        uploader._upload_files(upload_targets)
    
        assert mock_s3_resource.Bucket.return_value.upload_file.call_count == 2
    
    
    def test_upload_files_partial_failure(
        self, 
        uploader: HcpUploaderImpl, 
        mock_s3_resource: Mock, 
        caplog: Any
    ) -> None:
        upload_targets = [
            UploadTarget(local_path=Path("file1.txt"), s3_key="file1.txt"),
            UploadTarget(local_path=Path("file2.txt"), s3_key="file2.txt")
        ]
    
        error_response = {"Error": {"Code": "NoSuchBucket", "Message": "The bucket does not exist"}}
        mock_s3_resource.Bucket.return_value.upload_file.side_effect = [
            None,  # First upload succeeds
            ClientError(error_response, "PutObject")  # Second upload fails
        ]
    
        uploader._upload_files(upload_targets)
        assert "Failed uploads: 1 files" in caplog.text
    
    
    def test_upload_dir(
        self, 
        uploader: HcpUploaderImpl, 
        mock_s3_resource: Mock, 
        temp_dir: Path
    ) -> None:
        uploader.upload_dir(temp_dir)
    
        assert mock_s3_resource.Bucket.return_value.upload_file.call_count == 4
        
        calls = mock_s3_resource.Bucket.return_value.upload_file.call_args_list
        print(calls)
        uploaded_s3_keys = [call[0][1] for call in calls]
        assert set(uploaded_s3_keys) == {
            'subdir_1/file1_1',
             'subdir_1/file1_2',
             'subdir_2a/subdir_2b/file2_1',
             'subdir_2a/subdir_2b/file2_2'
        }