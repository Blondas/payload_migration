import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
from typing import Any
from unit_of_work.processor.unit_of_work_processor_impl import UnitOfWorkProcessorImpl
from unit_of_work.tape_register.tape_register import TapeRegister
from unit_of_work.slicer.slicer import Slicer
from unit_of_work.sanity_checker.sanity_checker import SanityChecker
from unit_of_work.linker.link_creator.link_creator import LinkCreator
from unit_of_work.uploader.hcp_uploader import HcpUploader
from unit_of_work.tape_import_confirmer.tape_import_confirmer import TapeImportConfirmer


class TestUnitOfWorkProcessorImpl:
    @pytest.fixture
    def processor(self) -> UnitOfWorkProcessorImpl:
        tape_register = MagicMock(spec=TapeRegister)
        tape_import_confirmer = MagicMock(spec=TapeImportConfirmer)
        slicer = MagicMock(spec=Slicer)
        sanity_checker = MagicMock(spec=SanityChecker)
        link_creator = MagicMock(spec=LinkCreator)
        hcp_uploader = MagicMock(spec=HcpUploader)

        slicer_output_directory = Path("/mock/slicer/output")
        slicer_log = Path("/mock/slicer/log")
        sanity_checker_log = Path("/mock/sanity/log")
        linker_output_directory = Path("/mock/linker/output")

        return UnitOfWorkProcessorImpl(
            tape_import_confirmer=tape_import_confirmer,
            tape_register=tape_register,
            slicer=slicer,
            sanity_checker=sanity_checker,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader,
            slicer_output_directory=slicer_output_directory,
            slicer_log=slicer_log,
            sanity_checker_log=sanity_checker_log,
            linked_output_directory=linker_output_directory
        )

    @patch('unit_of_work.processor.unit_of_work_processor_impl.delete_path')
    @patch('unit_of_work.processor.unit_of_work_processor_impl.time.time')
    def test_process_runs_all_steps_successfully(
        self,
        mock_time: MagicMock,
        mock_delete_path: MagicMock,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape1"
        tape_location = Path("/path/to/tape1")
        mock_confirmation_file = Path("/path/to/confirmation")
        processor._tape_import_confirmer.get_tape_confirmation_file.return_value = mock_confirmation_file
        mock_time.side_effect = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]  # Updated for 5 operations

        # When
        processor.process(tape_name, tape_location)

        # Then
        processor._tape_import_confirmer.wait_for_confirmation.assert_called_once_with(
            tape_name, tape_location
        )
        processor._slicer.execute.assert_called_once_with(
            tape_location=tape_location,
            output_directory=processor._slicer_output_directory,
            log_file=processor._slicer_log
        )
        processor._sanity_checker.execute.assert_called_once_with(
            tape_name=tape_name,
            slicer_log=processor._slicer_log,
            slicer_output_directory=processor._slicer_output_directory,
            sanity_checker_log=processor._sanity_checker_log
        )
        processor._link_creator.create_links.assert_called_once()
        processor._hcp_uploader.upload_dir.assert_called_once_with(
            processor._linker_output_directory
        )

        # Verify status updates
        processor._tape_register.set_status_exported.assert_called_once_with(tape_name)
        processor._tape_register.set_status_sliced.assert_called_once_with(tape_name)
        processor._tape_register.set_status_sanitized.assert_called_once_with(tape_name)
        processor._tape_register.set_status_linked.assert_called_once_with(tape_name)
        processor._tape_register.set_status_finished.assert_called_once_with(tape_name)
        processor._tape_register.set_status_failed.assert_not_called()

        # Verify cleanup calls
        expected_delete_calls = [
            call(processor._slicer_output_directory, False),  # During _run_linker
            call(processor._linker_output_directory, False),  # During _run_uploader
            call(processor._slicer_output_directory, True),  # During _clean_working_dir
            call(processor._linker_output_directory, True),  # During _clean_working_dir
            call(tape_location, True),  # During _clean_tape_and_tape_confirmation_file
            call(mock_confirmation_file, True)  # During _clean_tape_and_tape_confirmation_file
        ]
        assert mock_delete_path.call_count == 6
        mock_delete_path.assert_has_calls(expected_delete_calls, any_order=True)

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
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
        processor._sanity_checker.execute.assert_not_called()
        processor._link_creator.create_links.assert_not_called()
        processor._hcp_uploader.upload_dir.assert_not_called()

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
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
        processor._sanity_checker.execute.assert_not_called()
        processor._link_creator.create_links.assert_not_called()
        processor._hcp_uploader.upload_dir.assert_not_called()

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
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

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
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
        processor._tape_register.set_status_finished.assert_not_called()

    @patch('unit_of_work.processor.unit_of_work_processor_impl.delete_path')
    def test_clean_working_dir_deletes_all_directories(
        self,
        mock_delete_path: MagicMock,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # When
        processor._clean_working_dir()

        # Then
        expected_calls = [
            call(processor._slicer_output_directory, True),
            call(processor._linker_output_directory, True)
        ]
        assert mock_delete_path.call_count == 2
        mock_delete_path.assert_has_calls(expected_calls)

    # New test cases below

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
    def test_process_logs_error_and_sets_status_failed_on_sanity_checker_failure(
        self,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape6"
        tape_location = Path("/path/to/tape6")
        expected_error = "Sanity checker error"
        processor._sanity_checker.execute.side_effect = Exception(expected_error)

        # When
        processor.process(tape_name, tape_location)

        # Then
        expected_calls = [
            call(f"Sanity checker failed, tape name: {tape_name}, {expected_error}"),
            call(f"Unit of work failed, tape name: {tape_name}, {expected_error}")
        ]
        mock_logger.error.assert_has_calls(expected_calls)
        assert mock_logger.error.call_count == 2
        processor._tape_register.set_status_failed.assert_called_once_with(tape_name)
        processor._tape_register.set_status_sanitized.assert_not_called()
        processor._link_creator.create_links.assert_not_called()
        processor._hcp_uploader.upload_dir.assert_not_called()

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
    @patch('unit_of_work.processor.unit_of_work_processor_impl.delete_path')
    @patch('unit_of_work.processor.unit_of_work_processor_impl.time.time')
    def test_process_logs_statistics_on_success(
        self,
        mock_time: MagicMock,
        mock_delete_path: MagicMock,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape7"
        tape_location = Path("/path/to/tape7")
        mock_confirmation_file = Path("/path/to/confirmation")
        processor._tape_import_confirmer.get_tape_confirmation_file.return_value = mock_confirmation_file
        mock_time.side_effect = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]  # 5 operations

        # When
        processor.process(tape_name, tape_location)

        # Then
        expected_stats_call = call(
            f"Statistics for unit of work: "
            f"[tape={tape_name}] "
            f"[location={tape_location}] "
            f"[confirmer_wait=1.0] "
            f"[slicer=1.0] "
            f"[sanity_checker=1.0] "
            f"[linker=1.0] "
            f"[uploader=1.0]"
        )
        mock_logger.info.assert_any_call(f"Uploader finished, working directory deleted, tape name: {tape_name}")
        mock_logger.info.assert_any_call(expected_stats_call.args[0])
        assert mock_logger.info.call_count >= 6  # At least start messages + finish + stats

    @patch('unit_of_work.processor.unit_of_work_processor_impl.logger')
    def test_run_slicer_logs_specific_error_on_failure(
        self,
        mock_logger: Any,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape_name = "tape8"
        tape_location = Path("/path/to/tape8")
        expected_error = "Slicer specific error"
        processor._slicer.execute.side_effect = Exception(expected_error)

        # When
        with pytest.raises(Exception) as exc_info:
            processor._run_slicer(tape_name, tape_location)

        # Then
        mock_logger.error.assert_called_once_with(
            f"Slicer failed, tape name: {tape_name}, {expected_error}"
        )
        assert str(exc_info.value) == expected_error

    @patch('unit_of_work.processor.unit_of_work_processor_impl.delete_path')
    def test_clean_tape_and_confirmation_file_deletes_both(
        self,
        mock_delete_path: MagicMock,
        processor: UnitOfWorkProcessorImpl
    ) -> None:
        # Given
        tape = Path("/path/to/tape9")
        confirmation_file = Path("/path/to/confirmation9")

        # When
        processor._clean_tape_and_tape_confirmation_file(tape, confirmation_file)

        # Then
        expected_calls = [
            call(tape, True),
            call(confirmation_file, True)
        ]
        assert mock_delete_path.call_count == 2
        mock_delete_path.assert_has_calls(expected_calls)