from unittest.mock import MagicMock, patch
from pathlib import Path
from payload_migration.tape_processor.tape_processor_impl import TapeProcessorImpl
from payload_migration.db2.db_connection import DBConnection
from payload_migration.slicer.slicer import Slicer
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.uploader.hcp_uploader import HcpUploader

class TestProcessorFactoryImpl:
    @patch('payload_migration.tape_processor.tape_processor_impl.delete_path')
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
            linker_output_directory=Path('/target'),
            db_connection=db_connection,
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader,
            delete_output_tape_dir=True
        )
    
        processor.process_tape()
    
        mock_execute.assert_called_once()
        mock_create_links.assert_called_once()
        mock_upload_dir.assert_called_once()
        mock_delete_path.assert_called_once()
    
    @patch('payload_migration.tape_processor.tape_processor_impl.delete_path')
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
            linker_output_directory=Path('/target'),
            db_connection=db_connection,
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader,
            delete_output_tape_dir=False
        )
    
        processor.process_tape()
    
        mock_execute.assert_called_once()
        mock_create_links.assert_called_once()
        mock_upload_dir.assert_called_once()
        mock_delete_path.assert_not_called()

    @patch('payload_migration.tape_processor.tape_processor_impl.delete_path')
    @patch('payload_migration.slicer.slicer.Slicer.execute')
    @patch('payload_migration.linker.link_creator.link_creator.LinkCreator.create_links')
    @patch('payload_migration.uploader.hcp_uploader.HcpUploader.upload_dir')
    def test_tape_processing_with_missing_file(self, mock_upload_dir, mock_create_links, mock_execute,
                                               mock_delete_path):
        db_connection = MagicMock(spec=DBConnection)
        slicer = MagicMock(spec=Slicer)
        # Make execute raise FileNotFoundError
        mock_execute.side_effect = FileNotFoundError("Tape not found")
        slicer.execute = mock_execute
        link_creator = MagicMock(spec=LinkCreator)
        link_creator.create_links = mock_create_links
        hcp_uploader = MagicMock(spec=HcpUploader)
        hcp_uploader.upload_dir = mock_upload_dir

        processor = TapeProcessorImpl(
            slicer_log_dir=Path('/logs'),
            slicer_log_name='slicer.log',
            tape_location=Path('/nonexistent_tape'),
            slicer_output_directory=Path('/output'),
            linker_output_directory=Path('/target'),
            db_connection=db_connection,
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader,
            delete_output_tape_dir=True
        )

        processor.process_tape()

        # execute should be called and raise error
        mock_execute.assert_called_once()
        # other operations should not be called after the error
        mock_create_links.assert_not_called()
        mock_upload_dir.assert_not_called()
        mock_delete_path.assert_not_called()