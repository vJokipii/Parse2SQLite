import os
import datetime
import logging

IMPORT_LOG = os.path.join("logs", "import.log")

logging.basicConfig(filename=IMPORT_LOG, level=logging.INFO)

def log_import_event(import_type, event_type, message):
    if event_type not in ['INFO', 'ERROR', 'WARNING']:
        return
    
    log = f" DATE - {datetime.datetime.now()} | IMPORT - {import_type} | MESSAGE - {message}"
    match event_type:
        case 'INFO':
            logging.info(log)
        case 'ERROR':
            logging.error(log)
        case 'WARNING':
            logging.warning(log)