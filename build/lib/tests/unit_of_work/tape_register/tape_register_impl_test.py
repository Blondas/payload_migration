import unittest
from unittest.mock import MagicMock, patch
from unit_of_work.tape_register.tape_register_impl import TapeRegisterImpl
from unit_of_work.tape_register.tape_status import TapeStatus


class TestTapeRegisterImpl(unittest.TestCase):
    def setUp(self):
        self.db_connection = MagicMock()
        self.tape_register = TapeRegisterImpl(self.db_connection, "mig_taperegister")

    def test_set_status_exported_updates_status_to_exported(self):
        self.tape_register.set_status_exported("tape1")
        self.db_connection.update.assert_called_once_with(
            f"UPDATE mig_taperegister SET status = '{TapeStatus.EXPORTED}' WHERE volser = 'tape1'"
        )
        
    def test_set_status_failed_updates_status_to_failed(self):
        self.tape_register.set_status_failed("tape2")
        self.db_connection.update.assert_called_once_with(
            f"UPDATE mig_taperegister SET status = '{TapeStatus.FAILED}' WHERE volser = 'tape2'"
        )

    def test_set_status_sliced_updates_status_to_sliced(self):
        self.tape_register.set_status_sliced("tape3")
        self.db_connection.update.assert_called_once_with(
            f"UPDATE mig_taperegister SET status = '{TapeStatus.SLICED}' WHERE volser = 'tape3'"
        )

    def test_set_status_linked_updates_status_to_linked(self):
        self.tape_register.set_status_linked("tape4")
        self.db_connection.update.assert_called_once_with(
            f"UPDATE mig_taperegister SET status = '{TapeStatus.LINKED}' WHERE volser = 'tape4'"
        )

    def test_set_status_exported_updates_status_to_finished(self):
        self.tape_register.set_status_finished("tape5")
        self.db_connection.update.assert_called_once_with(
            f"UPDATE mig_taperegister SET status = '{TapeStatus.FINISHED}' WHERE volser = 'tape5'"
        )

    @patch('unit_of_work.tape_register.tape_register_impl.logger')
    def test_set_status_logs_error_and_raises_exception_on_failure(self, mock_logger):
        self.db_connection.update.side_effect = Exception("DB error")
        with self.assertRaises(Exception):
            self.tape_register.set_status_failed("tape6")
        mock_logger.error.assert_called_once_with(f"Failed to update status to {TapeStatus.FAILED} for tape tape6: DB error")