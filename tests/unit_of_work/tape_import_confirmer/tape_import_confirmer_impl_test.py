import pytest
from unittest.mock import patch
from pathlib import Path
from unit_of_work.tape_import_confirmer.tape_import_confirmer_impl import TapeImportConfirmerImpl

class TestTapeImportConfirmerImpl:
    tape_name: str = 'tape_name'
    path_to_tape: Path = Path('/path/to/tape')
    
    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_files_exist(self, mock_exists):
        mock_exists.side_effect = [True, True]
        confirmer = TapeImportConfirmerImpl('READY', 10, 1)
        confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape)

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_timeout(self, mock_exists):
        mock_exists.side_effect = [False, False]
        confirmer = TapeImportConfirmerImpl( 'READY', 1, 1)
        with pytest.raises(TimeoutError):
            confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape)

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_tape_file_exists_only(self, mock_exists):
        mock_exists.side_effect = [True, False] * 2
        confirmer = TapeImportConfirmerImpl( 'READY', 1, 1)
        with pytest.raises(TimeoutError):
            confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape)

    @patch('pathlib.Path.exists')
    def test_wait_for_confirmation_confirmation_file_exists_only(self, mock_exists):
        mock_exists.side_effect = [False, True] * 2
        confirmer = TapeImportConfirmerImpl('READY', 1, 1)
        with pytest.raises(TimeoutError):
            confirmer.wait_for_confirmation(self.tape_name, self.path_to_tape)

    def test_get_tape_confirmation_file_returns_correct_path(self) -> None:
        # Given
        confirmer = TapeImportConfirmerImpl('READY', 10, 1)
        tape_name = "test_tape"
        tape_location = Path("/path/to/test_tape")
        expected_path = tape_location.parent / f"{tape_name}READY"

        # When
        result = confirmer.get_tape_confirmation_file(tape_name, tape_location)

        # Then
        assert result == expected_path
        assert isinstance(result, Path)