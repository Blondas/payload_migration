import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from payload_migration.slicer.SlicerImpl import SlicerImpl
import subprocess

class TestSlicerImpl:
    @patch('payload_migration.slicer.SlicerImpl.os.access', return_value=True)
    @patch('payload_migration.slicer.SlicerImpl.subprocess.run')
    def test_execute_slicer_command_successfully(self, mock_run, mock_access):
        mock_run.return_value = MagicMock(stdout='Success', stderr='')
        slicer = SlicerImpl(Path('/path/to/slicer'))
        slicer.execute(Path('/path/to/tape'), 'collection', Path('/path/to/output'), Path('/path/to/log'))
        mock_run.assert_called_once()

    @patch('payload_migration.slicer.SlicerImpl.os.access', return_value=True)
    @patch('payload_migration.slicer.SlicerImpl.subprocess.run')
    def test_execute_slicer_command_with_warnings(self, mock_run, mock_access):
        mock_run.return_value = MagicMock(stdout='Success', stderr='Warning')
        slicer = SlicerImpl(Path('/path/to/slicer'))
        slicer.execute(Path('/path/to/tape'), 'collection', Path('/path/to/output'), Path('/path/to/log'))
        mock_run.assert_called_once()

    @patch('payload_migration.slicer.SlicerImpl.os.access', return_value=True)
    @patch('payload_migration.slicer.SlicerImpl.subprocess.run')
    def test_execute_slicer_command_failure(self, mock_run, mock_access):
        mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd', stderr='Error')
        slicer = SlicerImpl(Path('/path/to/slicer'))
        with pytest.raises(Exception):
            slicer.execute(Path('/path/to/tape'), 'collection', Path('/path/to/output'), Path('/path/to/log'))

    def test_slicer_path_not_executable(self):
        with pytest.raises(ValueError):
            SlicerImpl(Path('/path/to/non_executable_slicer'))
