from unittest.mock import MagicMock, patch
from pathlib import Path
from payload_migration.executor.parallel_executor import ParallelTapeExecutor
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory
import pytest


class TestParallelExecutor:

    @patch('payload_migration.executor.parallel_executor.ParallelTapeExecutor._process_tape')
    def test_processing_all_tapes(self, mock_process_tape):
        input_directory = MagicMock(spec=Path)
        # Create Path mocks that return True for is_file()
        mock_files = []
        for i in range(5):
            mock_file = MagicMock(spec=Path)
            mock_file.name = f'tape_{i}.txt'
            mock_file.is_file.return_value = True
            mock_files.append(mock_file)

        input_directory.glob.return_value = mock_files
        tape_processor_factory = MagicMock(spec=TapeProcessorFactory)

        executor = ParallelTapeExecutor(
            input_directory=input_directory,
            num_threads=3,
            tape_processor_factory=tape_processor_factory
        )

        executor.run()

        assert mock_process_tape.call_count == 5
    
    @patch('payload_migration.executor.parallel_executor.ParallelTapeExecutor._process_tape')
    def test_processing_no_tapes(self, mock_process_tape):
        input_directory = MagicMock(spec=Path)
        input_directory.glob.return_value = []
        tape_processor_factory = MagicMock(spec=TapeProcessorFactory)
    
        executor = ParallelTapeExecutor(
            input_directory=input_directory,
            num_threads=3,
            tape_processor_factory=tape_processor_factory
        )
    
        executor.run()
    
        mock_process_tape.assert_not_called()

    @patch('concurrent.futures.ThreadPoolExecutor')
    @patch('payload_migration.executor.parallel_executor.ParallelTapeExecutor._process_tape')
    def test_processing_with_exception(self, mock_process_tape, mock_executor_class):
        input_directory = MagicMock(spec=Path)
        # Create a proper mock file that returns True for is_file()
        mock_file = MagicMock(spec=Path)
        mock_file.name = 'tape_1.txt'
        mock_file.is_file.return_value = True
        input_directory.glob.return_value = [mock_file]

        tape_processor_factory = MagicMock(spec=TapeProcessorFactory)
        mock_process_tape.side_effect = Exception("Processing error")

        # Setup the executor mock to raise the exception
        mock_executor = MagicMock()
        mock_executor.map.side_effect = Exception("Processing error")
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        executor = ParallelTapeExecutor(
            input_directory=input_directory,
            num_threads=3,
            tape_processor_factory=tape_processor_factory
        )

        with pytest.raises(Exception, match="Processing error"):
            executor.run()