import logging

from payload_migration.db2.db_connection import DBConnection
from payload_migration.tape_register.tape_register import TapeRegister
from payload_migration.tape_register.tape_status import TapeStatus

logger = logging.getLogger(__name__)

class TapeRegisterImpl(TapeRegister):
    def __init__(
        self,
        _db_connection: DBConnection,
        tape_register_table: str
    ) -> None:
        self._db2_connection = _db_connection
        self._tape_register_table = tape_register_table
        
    def _set_status(self, tape_name: str, status: TapeStatus) -> None:
        query = f"UPDATE {self._tape_register_table} SET status = '{status}' WHERE volser = '{tape_name}'"
        try:
            self._db2_connection.update(query)
        except Exception as e:
            logger.error(f"Failed to update status to {status} for tape {tape_name}: {e}")
            raise

    def set_status_failed(self, tape_name: str) -> None:
        self._set_status(tape_name, TapeStatus.FAILED)

    def set_status_sliced(self, tape_name: str) -> None:
        self._set_status(tape_name, TapeStatus.SLICED)

    def set_status_linked(self, tape_name: str) -> None:
        self._set_status(tape_name, TapeStatus.LINKED)

    def set_status_exported(self, tape_name: str) -> None:
        self._set_status(tape_name, TapeStatus.EXPORTED)
        
    def set_status_finished(self, tape_name: str) -> None:
        self._set_status(tape_name, TapeStatus.FINISHED)

