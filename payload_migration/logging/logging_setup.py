import logging

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(extra)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )