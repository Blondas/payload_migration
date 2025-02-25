from contextlib import contextmanager
from typing import Generator, Optional, Dict

from ibm_db_dbi import connect, Connection, Error as DB2Error
import logging

from unit_of_work.db2.db_connection import DBConnection

logger = logging.getLogger(__name__)

class DB2ConnectionImpl(DBConnection):
    def __init__(
        self,
        database: str,
        user: str,
        password: str
    ):
        self._database = database
        self._user = user
        self._password = password

    @contextmanager
    def _connect(self) -> Generator[Connection, None, None]:
        conn: Optional[Connection] = None
        try:
            conn = connect(self._database, self._user, self._password)
            yield conn
        except DB2Error as e:
            logger.error("DB2 connection error",
                         extra={
                             'database': self._database,
                             'user': self._user,
                             'error_type': type(e).__name__
                         },
                         exc_info=e)
            raise
        except Exception as e:
            logger.error("Unexpected error during database connection",
                         extra={
                             'database': self._database,
                             'user': self._user,
                             'error_type': type(e).__name__
                         },
                         exc_info=e)
            raise
        finally:
            if conn is not None:
                conn.close()

    def fetch_all(self, query: str) -> Dict[str, tuple]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return {row[0]: (row[1],) if len(row) == 2 else row[1:]
                    for row in cursor.fetchall()}
        
    def fetch_one(self, query: str) -> Optional[tuple]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchone()

    def update(self, query: str) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()

