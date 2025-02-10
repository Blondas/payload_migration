import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from payload_migration.slicer.SlicerImpl import SlicerImpl
from payload_migration.slicer.collection_name_lookup.collection_name_lookup import CollectionNameLookup
from payload_migration.slicer.collection_name_lookup.collection_name_lookup_error import CollectionNameLookupError
import subprocess

@pytest.fixture
def mock_collection_name_lookup() -> MagicMock:
    return MagicMock(spec=CollectionNameLookup)

@pytest.fixture
def slicer_impl(mock_collection_name_lookup: MagicMock) -> SlicerImpl:
    with patch("os.access", return_value=True):
        return SlicerImpl(Path("/path/to/slicer"), mock_collection_name_lookup)

class TestSlicerImpl:
    def test_execute_success(self, mock_collection_name_lookup: MagicMock, slicer_impl: SlicerImpl):
        mock_collection_name_lookup.collection_name.return_value = "collection_name"
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")
    
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.stderr = None
    
            slicer_impl.execute(tape_location, output_directory, log_location)
    
            mock_collection_name_lookup.collection_name.assert_called_once_with("tape")
            mock_run.assert_called_once_with(
                ["/path/to/slicer", str(tape_location), "collection_name", str(output_directory)],
                check=True,
                text=True
            )
    
    def test_execute_collection_name_lookup_error(self, mock_collection_name_lookup: MagicMock, slicer_impl: SlicerImpl):
        mock_collection_name_lookup.collection_name.side_effect = CollectionNameLookupError("error")
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")
    
        with pytest.raises(CollectionNameLookupError):
            slicer_impl.execute(tape_location, output_directory, log_location)
    
    def test_execute_subprocess_error(self, mock_collection_name_lookup: MagicMock, slicer_impl: SlicerImpl):
        mock_collection_name_lookup.collection_name.return_value = "collection_name"
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")
    
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="error")
    
            with pytest.raises(Exception) as excinfo:
                slicer_impl.execute(tape_location, output_directory, log_location)
    
            assert "Slicer command failed: error" in str(excinfo.value)
    
    def test_execute_unexpected_error(self, mock_collection_name_lookup: MagicMock, slicer_impl: SlicerImpl):
        mock_collection_name_lookup.collection_name.return_value = "collection_name"
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")
    
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("unexpected error")
    
            with pytest.raises(Exception) as excinfo:
                slicer_impl.execute(tape_location, output_directory, log_location)
    
            assert "Unexpected error during Slicer command execution: unexpected error" in str(excinfo.value)