from typing import Dict

from payload_migration.db2.db2_connection import DB2Connection
from payload_migration.linker.agid_name_lookup.agid_name_lookup import AgidNameLookup


class AgidNameLookupImpl(AgidNameLookup):
    def __init__(
        self,
        db2_connection: DB2Connection
    ) -> None:
        self._db2_connection = db2_connection
        self._dict: Dict[str, str] = self._fetch()

    def _fetch(self) -> Dict[str, str]:
        with self._db2_connection.connect() as connection:
            query: str = "SELECT distinct(agid_name_src), agid_name_dst FROM mig_mapping"
            cursor = connection.cursor()
            cursor.execute(query)
            return dict(cursor.fetchall())

    def dest_agid_name(self, src_agid_name: str) -> str:
        default: str = "UNDEFINED"
        return self._dict.get(src_agid_name, default)
