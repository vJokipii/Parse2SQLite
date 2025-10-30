import os
import datetime
import logging

NOW = datetime.datetime.now()

def clean_old_logs(days=3):
    log_dir = "logs"
    if not os.path.exists(log_dir):
        return

    now = datetime.datetime.now()
    for filename in os.listdir(log_dir):
        filepath = os.path.join(log_dir, filename)
        if os.path.isfile(filepath):
            file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
            if (now - file_time).days >= days:
                try:
                    os.remove(filepath)
                    add_to_log('SYS', 'INFO', f"Deleted old log: {filename}")
                except Exception as e:
                    add_to_log('SYS', 'WARNING', f"Failed to delete old log -> {filename}: {e}")

def create_log_file():
    os.makedirs("logs", exist_ok=True)
    date = NOW.strftime("%d-%m-%Y")
    time = NOW.strftime("%H-%M-%S")
    filepath = os.path.join("logs", f"import_{date}_{time}.log")

    with open(filepath, 'w') as f:
        f.write(f"New log file created at {time}:\n")

    logging.basicConfig(filename = filepath, level = logging.INFO)

def add_to_log(import_type, event_type, message):
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

def log_no__changes(import_type):
    logging.info(f" DATE - {datetime.datetime.now()} | IMPORT - {import_type} | MESSAGE - No changes detected during import.")