import pytest
from unittest.mock import MagicMock
from payload_migration.slicer.collection_name_lookup.collection_name_lookup_impl import CollectionNameLookupImpl, CollectionNameLookupError
from payload_migration.db2.db_connection import DBConnection

@pytest.fixture
def mock_db_connection() -> MagicMock:
    connection = MagicMock(spec=DBConnection)
    return connection

class TestCollectionNameLookupImpl:
    def test_collection_name_returns_collection_name(self, mock_db_connection: MagicMock):
        mock_db_connection.fetch_one.return_value = ["collection_name"]
        lookup = CollectionNameLookupImpl(mock_db_connection)

        result = lookup.collection_name("TAPE_NAME")

        assert result == "collection_name"

    def test_collection_name_raises_error_when_no_collection_name_found(self, mock_db_connection: MagicMock):
        mock_db_connection.fetch_one.return_value = None
        lookup = CollectionNameLookupImpl(mock_db_connection)

        with pytest.raises(CollectionNameLookupError):
            lookup.collection_name("TAPE_NAME")