# logger_config.py
import logging
import os

def setup_logger():
    logger = logging.getLogger("ManifestDebugger")

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        log_file = 'manifest_debug.log'
        
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        
        logger.propagate = False
        
    return logger

logger = setup_logger()