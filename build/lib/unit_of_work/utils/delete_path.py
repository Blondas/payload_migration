import shutil
import os
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def delete_path(path: Path, synchronous: bool = True):
    if os.path.exists(path):
        logging.info(f"Deleting path {path} ...")
        
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        
        if synchronous:
            while os.path.exists(path):
                time.sleep(0.1)
            logging.info(f"Path {path} has been deleted.")
        logging.info(f"Path {path} scheduled for deletion.")
    else:
        logging.info(f"Path {path} does not exist.")