from pathlib import Path

from unittest.mock import Mock
import pytest

from payload_migration.linker.path_transformer import PathTransformerImpl

# Feature and scenarios defined directly in the Python file
FEATURE = """
Feature: Path Transformation
    As a system processor
    I want to transform paths according to specific rules
    So that files are organized in the correct structure
"""


# Fixtures
@pytest.fixture
def agid_name_lookup():
    mock_lookup = Mock()
    mock_lookup.dest_agid_name.side_effect = lambda x: {
        'AAG': 'SFB',
        'BBH': 'TGC'
    }.get(x, 'XXX')
    return mock_lookup



@pytest.fixture
def transformer(agid_name_lookup):
    return PathTransformerImpl(agid_name_lookup)


# Test Scenarios
class TestPathTransformation:
    """Path Transformation Tests"""

    @pytest.mark.describe("When transforming an object path")
    def test_transform_object_path(self, transformer, agid_name_lookup):
        """Should correctly transform object path with proper format"""
        # Given
        input_path = Path("/ars/data/spool/output/A12345/slicer/AAG.L123.FAAA")
        target_base = Path("/ars/data/spool/output/A12345/linker")

        # When
        result = transformer.transform(input_path, target_base)

        # Then
        expected = Path("/ars/data/spool/output/A12345/linker/SFB/123FAA/123FAAA")
        assert result == expected
        agid_name_lookup.dest_agid_name.assert_called_with("AAG")

    @pytest.mark.describe("When transforming a resource path")
    def test_transform_resource_path(self, transformer, agid_name_lookup):
        """Should correctly transform resource path with proper format"""
        # Given
        input_path = Path("/ars/data/spool/output/A12345/slicer/AAG.L123")
        target_base = Path("/ars/data/spool/output/A12345/linker")

        # When
        result = transformer.transform(input_path, target_base)

        # Then
        expected = Path("/ars/data/spool/output/A12345/linker/SFB/RES/123")
        assert result == expected
        agid_name_lookup.dest_agid_name.assert_called_with("AAG")

    @pytest.mark.describe("When handling invalid path format")
    def test_invalid_path_format(self, transformer):
        """Should raise ValueError for invalid path format"""
        # Given
        input_path = Path("/ars/data/spool/output/A12345/slicer/invalid_format")
        target_base = Path("/ars/data/spool/output/A12345/linker")

        # When/Then
        with pytest.raises(ValueError, match="Unsupported path type"):
            transformer.transform(input_path, target_base)

    @pytest.mark.describe("When transforming different path formats")
    @pytest.mark.parametrize("input_name,expected_suffix", [
        ("AAG.L123.FAAA", "123FAAA"),
        ("BBH.L456.FBBB", "456FBBB"),
        ("AAG.L789.FCCC", "789FCCC"),
    ])
    def test_different_path_formats(self, transformer, input_name, expected_suffix):
        """Should handle various path formats correctly"""
        # Given
        input_path = Path(f"/ars/data/spool/output/A12345/slicer/{input_name}")
        target_base = Path("/ars/data/spool/output/A12345/linker")

        # When
        result = transformer.transform(input_path, target_base)

        # Then
        assert result.name == expected_suffix