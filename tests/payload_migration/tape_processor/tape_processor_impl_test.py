from unittest.mock import MagicMock, patch
from pathlib import Path
from payload_migration.tape_processor.tape_processor_impl import TapeProcessorImpl
from payload_migration.db2.db_connection import DBConnection
from payload_migration.slicer.slicer import Slicer
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.uploader.hcp_uploader import HcpUploader


class TestTapeProcessorImpl:

    @patch('payload_migration.tape_processor.tape_processor_impl.delete_path')  # Changed this line
    @patch('payload_migration.slicer.slicer.Slicer.execute')
    @patch('payload_migration.linker.link_creator.link_creator.LinkCreator.create_links')
    @patch('payload_migration.uploader.hcp_uploader.HcpUploader.upload_dir')
    def test_tape_processing_with_deletion(self, mock_upload_dir, mock_create_links, mock_execute, mock_delete_path):
        db_connection = MagicMock(spec=DBConnection)

        slicer = MagicMock(spec=Slicer)
        slicer.execute = mock_execute

        link_creator = MagicMock(spec=LinkCreator)
        link_creator.create_links = mock_create_links

        hcp_uploader = MagicMock(spec=HcpUploader)
        hcp_uploader.upload_dir = mock_upload_dir

        processor = TapeProcessorImpl(
            slicer_log_dir=Path('/logs'),
            slicer_log_name='slicer.log',
            tape_location=Path('/tape'),
            slicer_output_directory=Path('/output'),
            linker_target_base_dir=Path('/target'),
            db_connection=db_connection,
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader
        )

        processor.process_tape(perform_deletion=True)

        mock_execute.assert_called_once()
        mock_create_links.assert_called_once()
        mock_upload_dir.assert_called_once()
        mock_delete_path.assert_called_once()

    @patch('payload_migration.tape_processor.tape_processor_impl.delete_path')  # Changed this line
    @patch('payload_migration.slicer.slicer.Slicer.execute')
    @patch('payload_migration.linker.link_creator.link_creator.LinkCreator.create_links')
    @patch('payload_migration.uploader.hcp_uploader.HcpUploader.upload_dir')
    def test_tape_processing_without_deletion(self, mock_upload_dir, mock_create_links, mock_execute, mock_delete_path):
        db_connection = MagicMock(spec=DBConnection)
        slicer = MagicMock(spec=Slicer)
        slicer.execute = mock_execute 

        link_creator = MagicMock(spec=LinkCreator)
        link_creator.create_links = mock_create_links  

        hcp_uploader = MagicMock(spec=HcpUploader)
        hcp_uploader.upload_dir = mock_upload_dir  

        processor = TapeProcessorImpl(
            slicer_log_dir=Path('/logs'),
            slicer_log_name='slicer.log',
            tape_location=Path('/tape'),
            slicer_output_directory=Path('/output'),
            linker_target_base_dir=Path('/target'),
            db_connection=db_connection,
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader
        )

        processor.process_tape(perform_deletion=False)

        mock_execute.assert_called_once()
        mock_create_links.assert_called_once()
        mock_upload_dir.assert_called_once()
        mock_delete_path.assert_not_called()