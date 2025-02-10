import logging

from payload_migration.db2.db_connection import DBConnection
from payload_migration.slicer.collection_name_lookup.collection_name_lookup import CollectionNameLookup
from payload_migration.slicer.collection_name_lookup.collection_name_lookup_error import CollectionNameLookupError

logger = logging.getLogger(__name__)

class CollectionNameLookupImpl(CollectionNameLookup):
    
    def __init__(
        self,
        _db_connection: DBConnection         
    ) -> None:
        self._db2_connection = _db_connection
    

    def collection_name(self, tape_name: str) -> str:
        query = (
            f"SELECT DISTINCT(n.description) "
            "FROM remtapevol t "
            "INNER JOIN arsnode n ON n.description LIKE '%' || TRIM(t.storgrp) || '%' "
            f"WHERE t.volser = '{tape_name}'"
        )
        maybe_collection_name = self._db2_connection.fetch_one(query)
        if maybe_collection_name is None:
            raise CollectionNameLookupError(f"Can not find collection name for tape: {tape_name}")
        
        logger.info(f"Collection name found for tape {tape_name} is {maybe_collection_name[0]}")
        return maybe_collection_name[0]
