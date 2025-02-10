from pathlib import Path

from payload_migration.linker.link_creator.link_creator_impl import LinkCreatorImpl
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.tape_processor.tape_processor import TapeProcessor
from payload_migration.tape_processor.tape_processor_impl import TapeProcessorImpl
from payload_migration.db2.db_connection import DBConnection
from payload_migration.db2.db2_connection_impl import DB2ConnectionImpl
from payload_migration.slicer.slicer import Slicer
from payload_migration.slicer.slicer_impl import SlicerImpl
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory
from payload_migration.config.payload_migration_config import PayloadMigrationConfig
from payload_migration.slicer.collection_name_lookup.collection_name_lookup import CollectionNameLookup
from payload_migration.slicer.collection_name_lookup.collection_name_lookup_impl import CollectionNameLookupImpl
from payload_migration.linker.agid_name_lookup.agid_name_lookup import AgidNameLookup
from payload_migration.linker.agid_name_lookup.agid_name_lookup_impl import AgidNameLookupImpl
from payload_migration.linker.path_transformer.path_transformer import PathTransformer
from payload_migration.linker.path_transformer.path_transformer_impl import PathTransformerImpl
from payload_migration.uploader.hcp_uploader_aws_cli import HcpUploaderAwsCliImpl

class TapeProcessorFactoryImpl(TapeProcessorFactory):
    def __init__(self,
        payload_migration_config: PayloadMigrationConfig
    ):
        self._payload_migration_config: PayloadMigrationConfig = payload_migration_config


    def create_tape_processor(self, tape_name: str) -> TapeProcessor:
        db2_connection: DBConnection = DB2ConnectionImpl(
            self._payload_migration_config.db_config.database_config,
            self._payload_migration_config.db_config.user,
            self._payload_migration_config.db_config.password
        )

        collection_name_lookup: CollectionNameLookup = CollectionNameLookupImpl(db2_connection)
        slicer: Slicer = SlicerImpl(
            self._payload_migration_config.slicer_config.slicer_path,
            collection_name_lookup
        )

        agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
        path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)

        hcp_uploader: HcpUploader = HcpUploaderAwsCliImpl(
            self._payload_migration_config.uploader_config.s3_bucket,
            self._payload_migration_config.uploader_config.s3_prefix,
            self._payload_migration_config.uploader_config.verify_ssl
        )
        
        tape_output_dir: Path = self._payload_migration_config.output_base_dir / tape_name
        
        link_creator: LinkCreator = LinkCreatorImpl(
            source_dir=tape_output_dir / self._payload_migration_config.slicer_config.output_subdir,
            target_base_dir=tape_output_dir / self._payload_migration_config.linker_config.output_subdir,
            file_patterns=self._payload_migration_config.linker_config.file_patterns,
            path_transformer=path_transformer
        )
        
        return TapeProcessorImpl(
            slicer=slicer,
            link_creator=link_creator,
            hcp_uploader=hcp_uploader,
            tape_location=self._payload_migration_config.input_dir / tape_name,  
            slicer_log_dir=tape_output_dir / self._payload_migration_config.logging_config.log_subdir,
            slicer_log_name='slicer.log',
            slicer_output_directory=tape_output_dir / self._payload_migration_config.slicer_config.output_subdir,
            linker_output_directory=tape_output_dir / self._payload_migration_config.linker_config.output_subdir,
            delete_output_tape_dir=self._payload_migration_config.delete_output_tape_dir
        )