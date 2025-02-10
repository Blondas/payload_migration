from pathlib import Path

from payload_migration.linker.link_creator.link_creator_impl import LinkCreatorImpl
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.linker.path_transformer.path_transformer import PathTransformer
from payload_migration.tape_processor.tape_processor import TapeProcessor
from payload_migration.tape_processor.tape_processor_impl import TapeProcessorImpl
from payload_migration.db2.db_connection import DBConnection
from payload_migration.slicer.slicer import Slicer
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory

class TapeProcessorFactoryImpl(TapeProcessorFactory):
    def __init__(self,
        db_connection: DBConnection,
        slicer: Slicer,
        path_transformer: PathTransformer,
        hcp_uploader: HcpUploader,
        linker_file_patterns: list[str],
        input_dir: Path,
        output_base_dir: Path,
        delete_output_tape_dir: bool,
        log_subdir: str,
        slicer_output_subdir: str, 
        linker_output_subdir: str
    ):
        self._db_connection: DBConnection= db_connection
        self._slicer: Slicer = slicer
        self._path_transformer: PathTransformer = path_transformer
        self._hcp_uploader: HcpUploader = hcp_uploader
        
        self._linker_file_patterns: list[str] = linker_file_patterns
        
        self._input_dir: Path = input_dir
        self._output_base_dir: Path = output_base_dir
        self._delete_output_tape_dir: bool = delete_output_tape_dir
        self._log_subdir: str = log_subdir
        self._slicer_output_subdir: str = slicer_output_subdir
        self._linker_output_subdir: str = linker_output_subdir


    def create_tape_processor(self, tape_name: str) -> TapeProcessor:
        tape_output_dir: Path = self._output_base_dir / tape_name
        
        link_creator: LinkCreator = LinkCreatorImpl(
            source_dir=tape_output_dir / self._slicer_output_subdir,
            target_base_dir=tape_output_dir / self._linker_output_subdir,
            file_patterns=self._linker_file_patterns,
            path_transformer=self._path_transformer
        )
        
        return TapeProcessorImpl(
            db_connection=self._db_connection,
            slicer=self._slicer,
            link_creator=link_creator,
            hcp_uploader=self._hcp_uploader,
            tape_location=self._input_dir / tape_name,  
            slicer_log_dir=tape_output_dir / self._log_subdir,
            slicer_log_name='slicer.log',
            slicer_output_directory=tape_output_dir / self._slicer_output_subdir,
            linker_output_directory=tape_output_dir / self._linker_output_subdir,
            delete_output_tape_dir=self._delete_output_tape_dir
        )