import logging
from typing import Dict

from payload_migration.db2.db_connection import DBConnection
from payload_migration.linker.agid_name_lookup.agid_name_lookup import AgidNameLookup

logger = logging.getLogger(__name__)

class RemagError(Exception):
    """Exception raised for missing entries in remag table."""
    pass


class AgidNameLookupImpl(AgidNameLookup):
    _QUERY: str = "SELECT distinct(agid_name_src), agid_name_dst FROM mig_mapping"
    
    def __init__(
        self,
        _db_connection: DBConnection         
    ) -> None:
        self._db2_connection = _db_connection
        
        self._dict: Dict[str, str] = {
            k: v[0] 
            for k, v in self._db2_connection.fetch_all(self._QUERY).items()
        }

    def dest_agid_name(self, src_agid_name: str) -> str:
        try:
            return self._dict[src_agid_name]
        except KeyError:
            logger.warning(f"Missing entry in remag table for src_agid_name: {src_agid_name}")
            raise RemagError(f"Missing entry in remag table for src_agid_name: {src_agid_name}")

