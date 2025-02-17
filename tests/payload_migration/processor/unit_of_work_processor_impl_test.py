import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
from typing import Any
from payload_migration.processor.unit_of_work_processor_impl import UnitOfWorkProcessorImpl
from payload_migration.config.payload_migration_config import PayloadMigrationConfig
from payload_migration.tape_register.tape_register import TapeRegister
from payload_migration.slicer.slicer import Slicer
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.tape_import_confirmer.tape_import_confirmer import TapeImportConfirmer


class TestUnitOfWorkProcessorImpl:
    @pytest.fixture
    def processor(self) -> UnitOfWorkProcessorImpl:
        config = MagicMock(spec=PayloadMigrationConfig)
        config.slicer_config = MagicMock()
        config.slicer_config.output_directory = Path("/mock/slicer/output")
        config.slicer_config.log_file = Path("/mock/slicer/log")
        config.linker_config = MagicMock()
        config.linker_config.output_directory = Path("/mock/linker/output")

        tape_register = MagicMock(spec=TapeRegister)
        tape_import_confirmer = MagicMock(spec=TapeImportConfirmer)
        slicer = MagicMock(spec=Slicer)
        link_creator = MagicMock(spec=LinkCreator)
        hcp_uploader = MagicMock(spec=HcpUploader)

        return UnitOfWorkProcessorImpl(
            config=config,
            tape_register=tape_register,
            tape_import_confirmer=tape_import_confirmer,
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader
        )

    @patch('payload_migration.processor.unit_of_work_processor_impl.delete_path')
    @patch('payload_migration.processor.unit_of_work_processor_impl.time.time')
    def test_process_runs_all_steps_successfully(
        self,
        mock_time: MagicMock,
        mock_delete_path: MagicMock,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape1"
        tape_location = Path("/path/to/tape1")
        mock_time.side_effect = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]  # Start/end times for 4 operations

        # When
        processor.process(tape_name, tape_location)

        # Then
        processor._tape_import_confirmer.wait_for_confirmation.assert_called_once_with(
            tape_name, tape_location
        )
        processor._slicer.execute.assert_called_once_with(
            tape_location=tape_location,
            output_directory=processor._config.slicer_config.output_directory,
            log_file=processor._config.slicer_config.log_file  # Changed from log_location to log_file
        )
        processor._link_creator.create_links.assert_called_once()
        processor._hcp_uploader.upload_dir.assert_called_once_with(
            processor._config.linker_config.output_directory
        )

        # Verify status updates
        processor._tape_register.set_status_sliced.assert_called_once_with(tape_name)
        processor._tape_register.set_status_linked.assert_called_once_with(tape_name)
        processor._tape_register.set_status_exported.assert_called_once_with(tape_name)
        processor._tape_register.set_status_failed.assert_not_called()

        # Verify cleanup calls
        assert mock_delete_path.call_count == 4  # Two during process + two in _clean_working_dir

    @patch('payload_migration.processor.unit_of_work_processor_impl.logger')
    def test_process_logs_error_and_sets_status_failed_on_confirmer_failure(
        self,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape5"
        tape_location = Path("/path/to/tape5")
        expected_error = "Confirmer error"
        processor._tape_import_confirmer.wait_for_confirmation.side_effect = Exception(expected_error)

        # When
        processor.process(tape_name, tape_location)

        # Then
        expected_calls = [
            call(
                f"Tape record confirmer wait failed, tape name: {tape_name}, tape location: {tape_location}, {expected_error}"),
            call(f"Unit of work failed, tape name: {tape_name}, {expected_error}")
        ]
        mock_logger.error.assert_has_calls(expected_calls)
        assert mock_logger.error.call_count == 2
        processor._tape_register.set_status_failed.assert_called_once_with(tape_name)
        processor._slicer.execute.assert_not_called()
        processor._link_creator.create_links.assert_not_called()
        processor._hcp_uploader.upload_dir.assert_not_called()

    @patch('payload_migration.processor.unit_of_work_processor_impl.logger')
    def test_process_logs_error_and_sets_status_failed_on_slicer_failure(
        self,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape2"
        tape_location = Path("/path/to/tape2")
        expected_error = "Slicer error"
        processor._slicer.execute.side_effect = Exception(expected_error)

        # When
        processor.process(tape_name, tape_location)

        # Then
        expected_calls = [
            call(f"Slicer failed, tape name: {tape_name}, {expected_error}"),
            call(f"Unit of work failed, tape name: {tape_name}, {expected_error}")
        ]
        mock_logger.error.assert_has_calls(expected_calls)
        assert mock_logger.error.call_count == 2
        processor._tape_register.set_status_failed.assert_called_once_with(tape_name)
        processor._tape_register.set_status_sliced.assert_not_called()
        processor._link_creator.create_links.assert_not_called()
        processor._hcp_uploader.upload_dir.assert_not_called()

    @patch('payload_migration.processor.unit_of_work_processor_impl.logger')
    def test_process_logs_error_and_sets_status_failed_on_linker_failure(
        self,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape3"
        tape_location = Path("/path/to/tape3")
        expected_error = "Linker error"
        processor._link_creator.create_links.side_effect = Exception(expected_error)

        # When
        processor.process(tape_name, tape_location)

        # Then
        expected_calls = [
            call(f"Linker failed, tape name: {tape_name} {expected_error}"),
            call(f"Unit of work failed, tape name: {tape_name}, {expected_error}")
        ]
        mock_logger.error.assert_has_calls(expected_calls)
        assert mock_logger.error.call_count == 2
        processor._tape_register.set_status_failed.assert_called_once_with(tape_name)
        processor._tape_register.set_status_linked.assert_not_called()
        processor._hcp_uploader.upload_dir.assert_not_called()

    @patch('payload_migration.processor.unit_of_work_processor_impl.logger')
    def test_process_logs_error_and_sets_status_failed_on_uploader_failure(
        self,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape4"
        tape_location = Path("/path/to/tape4")
        expected_error = "Uploader error"
        processor._hcp_uploader.upload_dir.side_effect = Exception(expected_error)

        # When
        processor.process(tape_name, tape_location)

        # Then
        expected_calls = [
            call(f"Uploader failed, tape name: {tape_name} {expected_error}"),
            call(f"Unit of work failed, tape name: {tape_name}, {expected_error}")
        ]
        mock_logger.error.assert_has_calls(expected_calls)
        assert mock_logger.error.call_count == 2
        processor._tape_register.set_status_failed.assert_called_once_with(tape_name)
        processor._tape_register.set_status_exported.assert_not_called()

    @patch('payload_migration.processor.unit_of_work_processor_impl.delete_path')
    def test_clean_working_dir_deletes_all_directories(
        self, 
        mock_delete_path: MagicMock,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # When
        processor._clean_working_dir()

        # Then
        expected_calls = [
            call(processor._config.slicer_config.output_directory, True),
            call(processor._config.linker_config.output_directory, True)
        ]
        assert mock_delete_path.call_count == 2
        mock_delete_path.assert_has_calls(expected_calls)