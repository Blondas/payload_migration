import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from payload_migration.slicer.slicer_impl import SlicerImpl

import subprocess


@pytest.fixture
def slicer_impl() -> SlicerImpl:
    with patch("os.access", return_value=True):
        return SlicerImpl(Path("/path/to/slicer"))

class TestSlicerImpl:
    def test_execute_success(self, slicer_impl: SlicerImpl):
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")

        with (
            patch("pathlib.Path.mkdir") as mock_mkdir, 
            patch("subprocess.run") as mock_run
        ):
            mock_run.return_value.stderr = None

            slicer_impl.execute(tape_location, output_directory, log_location)

            # Verify mkdir calls
            assert mock_mkdir.call_count == 2
            mock_mkdir.assert_any_call(parents=True, exist_ok=True)

            # Verify subprocess.run call
            mock_run.assert_called_once_with(
                ["/path/to/slicer", str(tape_location), str(output_directory), str(log_location)],
                check=True,
                text=True
            )

    def test_execute_subprocess_error(self, slicer_impl: SlicerImpl):
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")

        with (
            patch("pathlib.Path.mkdir") as mock_mkdir, 
            patch("subprocess.run") as mock_run
        ):
            mock_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="error")

            with pytest.raises(Exception) as excinfo:
                slicer_impl.execute(tape_location, output_directory, log_location)

            assert "Slicer command failed: error" in str(excinfo.value)

    def test_execute_unexpected_error(self, slicer_impl: SlicerImpl):
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")

        # Add mocks for both mkdir and subprocess.run
        with (
            patch("pathlib.Path.mkdir") as mock_mkdir, 
            patch("subprocess.run") as mock_run
        ):
            mock_run.side_effect = Exception("unexpected error")

            with pytest.raises(Exception) as excinfo:
                slicer_impl.execute(tape_location, output_directory, log_location)

            assert "Unexpected error during Slicer command execution: unexpected error" in str(excinfo.value)