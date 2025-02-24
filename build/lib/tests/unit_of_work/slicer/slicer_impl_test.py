import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from unit_of_work.slicer.slicer_impl import SlicerImpl
import subprocess


@pytest.fixture
def slicer_impl() -> SlicerImpl:
    with patch("os.access", return_value=True):
        return SlicerImpl(Path("/path/to/slicer"))

class TestSlicerImpl:
    @patch('unit_of_work.slicer.slicer_impl.logger')
    def test_execute_success(self, mock_logger: MagicMock, slicer_impl: SlicerImpl):
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

            # Verify subprocess.run call - updated to match implementation
            mock_run.assert_called_once_with(
                [
                    "/path/to/slicer", 
                    str(tape_location),
                    str(output_directory),
                    str(log_location)
                ],
                check=True,
                text=True,
                cwd=output_directory
            )

            # Verify logging
            mock_logger.info.assert_any_call(
                f"Executing slicer command: /path/to/slicer {tape_location} {output_directory} {log_location}"
            )
            mock_logger.info.assert_any_call("Command executed successfully")

    @patch('unit_of_work.slicer.slicer_impl.logger')
    def test_execute_subprocess_error(self, mock_logger: MagicMock, slicer_impl: SlicerImpl):
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
            mock_logger.error.assert_called_once_with("Slicer command failed: error")

    @patch('unit_of_work.slicer.slicer_impl.logger')
    def test_execute_unexpected_error(self, mock_logger: MagicMock, slicer_impl: SlicerImpl):
        tape_location = Path("/path/to/tape")
        output_directory = Path("/path/to/output")
        log_location = Path("/path/to/log")

        with (
            patch("pathlib.Path.mkdir") as mock_mkdir, 
            patch("subprocess.run") as mock_run
        ):
            mock_run.side_effect = Exception("unexpected error")

            with pytest.raises(Exception) as excinfo:
                slicer_impl.execute(tape_location, output_directory, log_location)

            assert "Unexpected error during Slicer command execution: unexpected error" in str(excinfo.value)
            mock_logger.error.assert_called_once_with(
                "Unexpected error during Slicer command execution: unexpected error"
            )

    def test_init_raises_value_error_if_slicer_not_executable(self):
        with patch("os.access", return_value=False):
            with pytest.raises(ValueError) as excinfo:
                SlicerImpl(Path("/path/to/slicer"))
            assert "Slicer path /path/to/slicer is not executable" in str(excinfo.value)

    def test_get_tape_name_returns_tape_name(self, slicer_impl: SlicerImpl):
        tape_location = Path("/path/to/tape_file.tar")
        assert slicer_impl._get_tape_name(tape_location) == "tape_file.tar"