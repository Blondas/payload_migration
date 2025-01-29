from pathlib import Path
from typing import Dict, Optional
from unittest.mock import Mock, patch
import pytest

from payload_migration.linker.link_creator.link_creator_impl import LinkCreatorImpl

FEATURE = """
Feature: Symlink Creation
    As a system processor
    I want to create links according to transformed paths
    So that files are properly linked in the target structure
"""

@pytest.fixture
def link_creator(
    source_dir: Path,
    target_base_dir: Path,
    file_patterns: list[str],
    path_transformer: Mock
) -> LinkCreatorImpl:
    return LinkCreatorImpl(
        source_dir=source_dir,
        target_base_dir=target_base_dir,
        file_patterns=file_patterns,
        path_transformer=path_transformer
    )

@pytest.fixture
def source_dir(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    return source

@pytest.fixture
def target_base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "target"
    target.mkdir()
    return target

@pytest.fixture
def file_patterns(request) -> list[str]:  # Changed to use request
    return getattr(request, 'param', ["*"])

@pytest.fixture
def path_transformer(target_base_dir) -> Mock:
    mock_transformer = Mock()

    def transform_path(path: Path, target_base_dir: Path) -> Path:
        target_path = target_base_dir / path.name
        return target_path

    mock_transformer.transform.side_effect = transform_path
    return mock_transformer

class TestLinkCreator:
    def test_create_hardlinks_success(
        self,
        link_creator: LinkCreatorImpl,
        source_dir: Path,
        target_base_dir: Path,
        path_transformer: Mock
    ) -> None:
        """Should successfully create hardlinks for all valid source files"""
        # Given
        source_file = source_dir / "test.txt"
        source_file.touch()
        expected_target = target_base_dir / "test.txt"

        # When

        results: Dict[Path, Optional[Exception]] = link_creator.create_links()

        # Then
        assert len(results) == 1
        assert results[source_file] is None
        path_transformer.transform.assert_called_once_with(source_file, target_base_dir)
        assert expected_target.exists()
        assert expected_target.stat().st_nlink == 2
        assert expected_target.stat().st_ino == source_file.stat().st_ino  # Verify same inode



    def test_create_hardlinks_target_exists(
        self,
        link_creator: LinkCreatorImpl,
        source_dir: Path,
        target_base_dir: Path
    ) -> None:
        """Should handle error when target path already exists"""
        # Given
        source_file = source_dir / "test.txt"
        source_file.touch()
        target_base_dir.mkdir(parents=True, exist_ok=True)
        (target_base_dir / "test.txt").touch()

        # When
        results = link_creator.create_links()

        # Then
        assert len(results) == 1
        assert isinstance(results[source_file], FileExistsError)

    def test_create_symlinks_empty_source(
        self,
        link_creator: LinkCreatorImpl
    ) -> None:
        """Should handle empty source directory gracefully"""
        # When
        results = link_creator.create_links()

        # Then
        assert len(results) == 0

    @pytest.mark.parametrize(
        "file_patterns",
        [[
            '[A-Z0-9]*.[A-Z0-9]*',
            '[A-Z0-9]*.[A-Z0-9]*.[A-Z0-9]*'
        ]],
        indirect=True
    )
    def test_create_symlinks_pattern_matching(
        self,
        link_creator: LinkCreatorImpl,
        source_dir: Path,
        file_patterns: list[str]
    ) -> None:
        """Should only process files matching the specified pattern"""
        # Given
        txt_file1 = source_dir / "AAA.BBB.CCC"
        txt_file1.touch()
        txt_file2 = source_dir / "AAA.BBB"
        txt_file2.touch()
        other_file = source_dir / "AAA"
        other_file.touch()

        # When
        results = link_creator.create_links()

        # Then
        assert len(results) == 2
        assert txt_file1 in results
        assert txt_file2 in results
        assert other_file not in results

    @patch('payload_migration.linker.link_creator.link_creator_impl.logger')
    def test_create_symlinks_transformation_error(
        self,
        mock_logger: Mock,
        link_creator: LinkCreatorImpl,
        source_dir: Path,
        path_transformer: Mock
    ) -> None:
        """Should handle path transformation errors gracefully"""
        # Given
        source_file = source_dir / "test.txt"
        source_file.touch()
        
        path_transformer.transform.side_effect = ValueError("Invalid path")

        # When
        results = link_creator.create_links()

        # Then
        assert len(results) == 1
        assert isinstance(results[source_file], ValueError)
        mock_logger.error.assert_called_once()
        print(mock_logger.error.call_args)
        