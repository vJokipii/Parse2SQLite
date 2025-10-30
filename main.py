from modules import handle_xml, handle_csv, event_logging as log


log.create_log_file()
log.clean_old_logs()
print(handle_xml.do_xml_update())
print(handle_csv.do_csv_update())