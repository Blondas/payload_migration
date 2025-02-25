import pytest
from unittest.mock import patch, Mock
from pathlib import Path
from unit_of_work.tape_import_confirmer.tape_import_confirmer_impl import TapeImportConfirmerImpl
from unit_of_work.tape_register.tape_status import TapeStatus


class TestTapeImportConfirmerImpl:
    tape_name: str = 'tape_name'
    path_to_tape: Path = Path('/path/to/tape')
    tape_status: TapeStatus = TapeStatus.EXPORTED

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_files_exist(self, mock_exists):
        mock_exists.side_effect = [True, True]
        confirmer = TapeImportConfirmerImpl(10, 1)

        # Create a patched version of get_tape_confirmation_file that doesn't require tape_status
        original_method = confirmer.get_tape_confirmation_file
        confirmer.get_tape_confirmation_file = lambda name, location: original_method(name, location, self.tape_status)

        confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape, self.tape_status)

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_timeout(self, mock_exists):
        mock_exists.side_effect = [False, False]
        confirmer = TapeImportConfirmerImpl(1, 1)

        # Create a patched version of get_tape_confirmation_file that doesn't require tape_status
        original_method = confirmer.get_tape_confirmation_file
        confirmer.get_tape_confirmation_file = lambda name, location: original_method(name, location, self.tape_status)

        with pytest.raises(TimeoutError):
            confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape, self.tape_status)

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_tape_file_exists_only(self, mock_exists):
        mock_exists.side_effect = [True, False] * 2
        confirmer = TapeImportConfirmerImpl(1, 1)

        # Create a patched version of get_tape_confirmation_file that doesn't require tape_status
        original_method = confirmer.get_tape_confirmation_file
        confirmer.get_tape_confirmation_file = lambda name, location: original_method(name, location, self.tape_status)

        with pytest.raises(TimeoutError):
            confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape, self.tape_status)

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_confirmation_file_exists_only(self, mock_exists):
        mock_exists.side_effect = [False, True] * 2
        confirmer = TapeImportConfirmerImpl(1, 1)

        # Create a patched version of get_tape_confirmation_file that doesn't require tape_status
        original_method = confirmer.get_tape_confirmation_file
        confirmer.get_tape_confirmation_file = lambda name, location: original_method(name, location, self.tape_status)

        with pytest.raises(TimeoutError):
            confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape, self.tape_status)

    def test_get_tape_confirmation_file_returns_correct_path(self) -> None:
        # Given
        confirmer = TapeImportConfirmerImpl(10, 1)
        tape_name = "test_tape"
        tape_location = Path("/path/to/test_tape")
        tape_status = TapeStatus.EXPORTED
        expected_path = tape_location.parent / f"{tape_name}{tape_status}"

        # When
        result = confirmer.get_tape_confirmation_file(tape_name, tape_location, tape_status)

        # Then
        assert result == expected_path
        assert isinstance(result, Path)