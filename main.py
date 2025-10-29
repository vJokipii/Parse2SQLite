import modules.handle_xml as handle_xml
import modules.handle_csv as handle_csv

if __name__ == "__main__":
    print(handle_xml.do_xml_update())
    print(handle_csv.do_csv_update())