from pathlib import Path
import re
from typing import Final
import logging

from payload_migration.linker.agid_name_lookup import AgidNameLookup
from payload_migration.linker.path_transformer import PathTransformer

logger = logging.getLogger(__name__)

class PathTransformerImpl(PathTransformer):
    RESOURCE_DIR: Final[str] = "RES"

    def __init__(
        self,
        agid_name_lookup: AgidNameLookup,
        step_name: str
    ):
        self._lookup = agid_name_lookup
        self._step_name = step_name

    # object:
    # in:  {output_dir}/{tape_id}/{PREV_STEP_NAME}  /{agid_name_src}.{load_id}.{load_id_suffix}
    # out: {output_dir}/{tape_id}/{linker}          /{agid_name_dst}/{load_id[1:]}{load_id_suffix[:3]}/{load_id[1:]}{load_id_suffix}
    #
    # resource:
    # in:  {output_dir}/{tape_id}/{PREV_STEP_NAME}  /{agid_name_src}.{load_id}.{load_id_suffix}
    # out: {output_dir}/{tape_id}/{linker}          /{agid_name_dst}/RES/{load_id[1:]}
    def transform(self, path: Path, target_base_dir: Path) -> Path:

        # in:  /ars/data/spool/output/A12345/slicer/AAG.L123.FAAA
        # out: /ars/data/spool/output/A12345/linker/SFB/123FAA/123FAAB
        is_object: bool = bool(re.match(r"\w+\.\w+\.\w+", path.name))
        is_resource: bool = bool(re.match(r"\w+\.\w+", path.name))

        if is_object:
            agid_name_src, load_id, load_id_suffix = path.name.split(".")
            agid_name_dst: str = self._lookup.dest_agid_name(agid_name_src)

            return (
                target_base_dir
                / agid_name_dst
                / (load_id[1:] + load_id_suffix[:3])
                / (load_id[1:] + load_id_suffix)
            )

        elif is_resource:
            agid_name_src, load_id = path.name.split(".")
            agid_name_dst: str = self._lookup.dest_agid_name(agid_name_src)

            return (
                target_base_dir
                / agid_name_dst
                / self.RESOURCE_DIR
                / load_id[1:]
            )

        else:
            error = ValueError(f"Unsupported path type: {path}")
            logger.error(
                "Unsupported path type",
                extra={
                    "path": path
                },
                exc_info=error
            )
            raise error