import unittest
from unittest.mock import MagicMock, patch
import pytest

from payload_migration.db2.db_connection import DBConnection
from payload_migration.linker.agid_name_lookup.agid_name_lookup_impl import AgidNameLookupImpl, RemagError

@pytest.fixture
def mock_db_connection() -> MagicMock:
    connection = MagicMock(spec=DBConnection)
    connection.fetch.return_value = {
        "src1": ("dest1",),
        "src2": ("dest2",),
        "src3": ("dest3",)
    }
    return connection

class TestAgidNameLookupImpl:

    def test_initialization_fetches_and_transforms_data(
        self,
        mock_db_connection: MagicMock
    ) -> None:
        # Given/When
        lookup = AgidNameLookupImpl(mock_db_connection)
    
        # Then
        mock_db_connection.fetch.assert_called_once_with(
            "SELECT distinct(agid_name_src), agid_name_dst FROM mig_mapping"
        )
        assert lookup._dict == {
            "src1": "dest1",
            "src2": "dest2",
            "src3": "dest3"
        }
    
    
    def test_dest_agid_name_returns_correct_mapping(
        self,
        mock_db_connection: MagicMock
    ) -> None:
        # Given
        lookup = AgidNameLookupImpl(mock_db_connection)

        # When
        result = lookup.dest_agid_name("src1")

        # Then
        assert result == "dest1"


    @patch('payload_migration.linker.agid_name_lookup.agid_name_lookup_impl.logger')
    def test_dest_agid_name_handles_missing_entry(
        self,
        mock_logger: MagicMock,
        mock_db_connection: MagicMock
    ) -> None:
        # Given
        lookup = AgidNameLookupImpl(mock_db_connection)
        unknown_src = "unknown_src"

        # When/Then
        with pytest.raises(RemagError) as exc_info:
            lookup.dest_agid_name(unknown_src)

        assert str(exc_info.value) == f"Missing entry in remag table for src_agid_name: {unknown_src}"

        mock_logger.warning.assert_called_once_with(
            f"Missing entry in remag table for src_agid_name: {unknown_src}"
        )


    def test_handles_empty_fetch_result(self) -> None:
        # Given
        connection = MagicMock(spec=DBConnection)
        connection.fetch.return_value = {}

        # When
        lookup = AgidNameLookupImpl(connection)

        # Then
        assert lookup._dict == {}


    def test_initialization_with_multi_value_tuples(self) -> None:
        # Given
        connection = MagicMock(spec=DBConnection)
        connection.fetch.return_value = {
            "src1": ("dest1", "extra1", "more1"),
            "src2": ("dest2", "extra2")
        }

        # When
        lookup = AgidNameLookupImpl(connection)

        # Then
        assert lookup._dict == {
            "src1": "dest1",
            "src2": "dest2"
        }


    def test_query_constant_value(self) -> None:
        # Given/When/Then
        assert AgidNameLookupImpl._QUERY == (
            "SELECT distinct(agid_name_src), agid_name_dst FROM mig_mapping"
        )