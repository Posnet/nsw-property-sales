#!/usr/bin/env python3
import os
import shutil
import zipfile
import hashlib
from datetime import datetime
from urllib import request, error
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
import re


def download_file(url, directory, progress_update):
    try:
        file_name = url.split("/")[-1]
        file_path = os.path.join(directory, file_name)
        with request.urlopen(url) as response, open(file_path, "wb") as out_file:
            data = response.read()
            out_file.write(data)
        progress_update(len(data))
        return file_path, len(data)
    except error.HTTPError as e:
        print(f"HTTP Error: {e.code} {e.reason} {url}")
    except error.URLError as e:
        print(f"URL Error: {e.reason} {url}")
    return None, 0


def extract_zip(file_path, target_path):
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(target_path)
        os.remove(file_path)
    except zipfile.BadZipFile:
        print(f"Failed to extract {file_path}, not a zip file.")


def file_hash(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def progress_tracker(total, operation="Downloading"):
    progress = [0]
    bytes_downloaded = [0]
    start_time = [time.time()]

    def update_progress(_bytes=0):
        progress[0] += 1
        elapsed_time = time.time() - start_time[0]
        percentage = (progress[0] / total) * 100
        if _bytes is not None:
            bytes_downloaded[0] += _bytes
            throughput = (bytes_downloaded[0] / 1024 / 1024) / elapsed_time
            throughput = f" - {throughput:.2f} MB/s"
            btotal = bytes_downloaded[0] / 1024 / 1024
            if btotal > 1024:
                btotal = f" - {btotal/1024:.2f}GB"
            else:
                btotal = f" - {btotal:.1f}MB"
                
        else:
            throughput = ""
            btotal = ""

        print(
            f"\r{operation} files: {progress[0]}/{total} ({percentage:.2f}%){throughput}{btotal}",
            end="",
            flush=True,
        )
        if progress[0] == total:
            print()

    return update_progress


def process_downloaded_files(extracted_path):
    zip_found = True
    seen = set()
    while zip_found:
        zip_found = False
        for root, _, files in os.walk(extracted_path):
            for file in files:
                src_path = os.path.join(root, file)
                if src_path not in seen:
                    print(src_path)
                    seen.add(src_path)
                if file.endswith(".DAT") or file.endswith(".zip"):
                    dst_dir = "data" if file.endswith(".DAT") else extracted_path
                    index = 0
                    base, extension = os.path.splitext(file)
                    dst_path = os.path.join(dst_dir, f"{base}_{index}{extension}")
                    while os.path.exists(dst_path):
                        dst_path = os.path.join(dst_dir, f"{base}_{index}{extension}")
                        index += 1
                    shutil.move(src_path, dst_path)
                    if file.endswith(".zip"):
                        extract_zip(dst_path, extracted_path)
                        zip_found = True


def fetch_sales_data(url, headers):
    req = request.Request(url, headers=headers)
    with request.urlopen(req) as response:
        html = response.read().decode("utf-8")
    return re.findall('href="([^"]+(zip|pdf))"', html)


def setup_directories():
    directories = ["pdfs", "data", "extracted"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    return directories


CODE_TO_ZONE = {
    "A": "Residential",
    "B": "Business",
    "C": "Sydney Commercial / Business",
    "D": "10(a) Sustainable Mixed Use Development",
    "E": "Employment",
    "I": "Industrial",
    "M": "9(a)(Mixed Residential / Business)",
    "N": "National Parks",
    "O": "Open Space",
    "P": "Protection",
    "R": "Non-Urban",
    "S": "Special Uses",
    "T": "North Sydney Commercial / Business",
    "U": "Community Uses",
    "V": "Comprehensive Centre",
    "W": "Reserve Open Space",
    "X": "Reserved Roads",
    "Y": "Reserved Special Uses",
    "Z": "Undetermined or Village",
    "RU1": "Primary Production",
    "RU2": "Rural Landscape",
    "RU3": "Forestry",
    "RU4": "Rural Small Holdings",
    "RU5": "Village",
    "RU6": "Transition",
    "R1": "General Residential",
    "R2": "Low Density Residential",
    "R3": "Medium Density Residential",
    "R4": "High Density Residential",
    "R5": "Large Lot Residential",
    "B1": "Neighbourhood Centre",
    "B2": "Local Centre",
    "B3": "Commercial Core",
    "B4": "Mixed Use",
    "B5": "Business Development",
    "B6": "Enterprise Corridor",
    "B7": "Business Park",
    "IN1": "General Industrial",
    "IN2": "Light Industrial",
    "IN3": "Heavy Industrial",
    "IN4": "Working Waterfront",
    "SP1": "Special Activities",
    "RE1": "Public Recreation",
    "RE2": "Private Recreation",
    "E1": "National Parks and Nature",
    "E2": "Environmental Conservation",
    "E3": "Environmental Management",
    "E4": "Environmental Living",
    "W1": "Natural Waterways",
    "W2": "Recreational Waterways",
    "W3": "Working Waterways",
}

CODE_TO_DISTRICT = {
    "050": "ALBURY",
    "257": "ARMIDALE REGIONAL",
    "148": "BALLINA",
    "230": "BALRANALD",
    "608": "BATHURST REGIONAL",
    "276": "BAYSIDE",
    "018": "BEGA VALLEY",
    "149": "BELLINGEN",
    "051": "BERRIGAN",
    "214": "BLACKTOWN",
    "231": "BLAND",
    "118": "BLAYNEY",
    "216": "BLUE MOUNTAINS",
    "232": "BOGAN",
    "239": "BOURKE",
    "233": "BREWARRINA",
    "234": "BROKEN HILL",
    "137": "BURWOOD",
    "150": "BYRON",
    "109": "CABONNE",
    "217": "CAMDEN",
    "218": "CAMPBELLTOWN",
    "139": "CANADA BAY",
    "258": "CANTERBURY-BANKSTOWN",
    "052": "CARRATHOOL",
    "259": "CENTRAL COAST",
    "235": "CENTRAL DARLING",
    "001": "CESSNOCK",
    "260": "CITY OF PARRAMATTA",
    "708": "CITY OF SYDNEY",
    "303": "CLARENCE VALLEY",
    "236": "COBAR",
    "152": "COFFS HARBOUR",
    "054": "COOLAMON",
    "238": "COONAMBLE",
    "042": "COWRA",
    "261": "CUMBERLAND",
    "275": "DUBBO REGIONAL",
    "002": "DUNGOG",
    "262": "EDWARD RIVER",
    "097": "EUROBODALLA",
    "220": "FAIRFIELD",
    "263": "FEDERATION",
    "117": "FORBES",
    "264": "GEORGES RIVER",
    "265": "COOTAMUNDRA-GUNDAGAI REGIONAL",
    "240": "GILGANDRA",
    "302": "GLEN INNES SEVERN",
    "529": "GOULBURN MULWAREE",
    "560": "GREATER HUME",
    "074": "GRIFFITH",
    "187": "GUNNEDAH",
    "300": "GWYDIR",
    "219": "HAWKESBURY",
    "243": "HAY",
    "266": "HILLTOPS",
    "082": "HORNSBY",
    "083": "HUNTERS HILL",
    "267": "INNER WEST",
    "188": "INVERELL",
    "061": "JUNEE",
    "157": "KEMPSEY",
    "098": "KIAMA",
    "084": "KU-RING-GAI",
    "158": "KYOGLE",
    "244": "LACHLAN",
    "004": "LAKE MACQUARIE",
    "085": "LANE COVE",
    "065": "LEETON",
    "159": "LISMORE",
    "222": "LITHGOW",
    "223": "LIVERPOOL",
    "301": "LIVERPOOL PLAINS",
    "066": "LOCKHART",
    "005": "MAITLAND",
    "620": "MID WESTERN REGIONAL",
    "268": "MID-COAST",
    "192": "MOREE PLAINS",
}


def parse_1990_file(file_path):
    # Initialize containers for different types of records
    data = {"HEADER": None, "SALES": [], "FOOTER": None}

    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(";")
            record_type = parts[0]

            if record_type == "A":  # Header record
                data["HEADER"] = {
                    "record_type": parts[0],
                    "district_code": parts[1],
                    "source": parts[2],
                    "download_datetime": parts[3],
                    "submitter_user_id": parts[4],
                }
            elif record_type == "B":  # Sales record
                area_type = {"M": "Square Meters", "H": "Hectares"}.get(
                    parts[14], parts[14]
                )
                contract_date = "".join(parts[10].split("/")[::-1])
                data["SALES"].append(
                    {
                        # 'record_type': parts[0],
                        "district_code": parts[1],
                        "district_name": CODE_TO_DISTRICT.get(parts[1].strip(), ""),
                        "property_id": parts[4],  # Removed the period
                        # 'source': parts[2],
                        # 'valuation_num': parts[3],
                        # "sale_counter": "",
                        "file_datetime": data["HEADER"]["download_datetime"],
                        "property_name": "",
                        "property_unit_number": parts[5],
                        "property_house_number": parts[6],
                        "property_street_name": parts[7],
                        "property_locality": parts[8],
                        "property_post_code": parts[9],
                        "area": parts[13],
                        "area_type": area_type,
                        "contract_date": contract_date,
                        "settlement_date": contract_date,
                        "purchase_price": parts[11],
                        "zone_code": parts[17],
                        "zone_name": CODE_TO_ZONE.get(parts[17].strip(), ""),
                        "nature_property": "",
                        "primary_purpose": "",
                        "strata_number": "",
                        "component_code": parts[16],
                        "sale_code": "",
                        "interest_sale": "",
                        "dealing_number": "",
                        "property_description": parts[12],
                        "purchaser_vendor": "",
                        "dimensions": parts[15],
                        "filetype": "archive",
                    }
                )
            elif record_type == "Z":  # Footer record
                data["FOOTER"] = {
                    "record_type": parts[0],
                    "total_records": parts[1],
                    "total_B_records": parts[2],
                }

    return data


def parse_sales_data_file(file_path):
    with open(file_path, "r") as file:
        data = {"HEADER": None, "FOOTER": None, "SALES": []}
        sales_index = {}  # To index sales entries by the first 5 columns

        counter = {"A": 0, "B": 0, "C": 0, "D": 0, "Z": 0}
        for line in file:
            line = line.strip()
            if line == "" or set(line) == {";"}:
                continue
            parts = line.split(";")
            segments = len(parts)
            record_type = parts[0]

            key = tuple(parts[1:5])  # First 5 columns as a tuple to use as a key

            counter[record_type] += 1
            if record_type == "A":  # Header
                if segments == 5:
                    parts = [parts[0]] + ["NA"] + parts[1:]
                if len(parts) != 6:
                    raise ValueError("Invalid File Header:", line)

                data["HEADER"] = {
                    "file_type": parts[1],
                    "district_code": parts[2],
                    "download_datetime": parts[3],
                    "submitter_user_id": parts[4],
                }
            elif record_type == "Z":  # Footer
                data["FOOTER"] = {
                    "total_records": parts[1],
                    "total_B_records": parts[2],
                    "total_C_records": parts[3],
                    "total_D_records": parts[4],
                }
            elif record_type == "B":  # New sale entry
                if len(data['SALES']) > 0:
                    last = data['SALES'][-1]
                    last_desc = last['property_description']
                    if isinstance(last_desc, list):  
                        last['property_description'] = "".join(last_desc)
                        
                    last_perch = last['purchaser_vendor']
                    if isinstance(last_perch, list):  
                        last['purchaser_vendor'] = ", ".join(last_perch)
                
                
                area_type = {"M": "Square Meters", "H": "Hectares"}.get(
                    parts[12], parts[12]
                )
                nature_property = {"V": "Vacant", "R": "Residence", "3": "Other"}.get(
                    parts[17], parts[17]
                )
                sales_index[key] = {
                    # 'Record Type': parts[0],
                    "district_code": parts[1],
                    "district_name": CODE_TO_DISTRICT.get(parts[1].strip(), ""),
                    "property_id": parts[2],
                    # "sale_counter": parts[3],
                    "file_datetime": parts[4],
                    "property_name": parts[5],
                    "property_unit_number": parts[6],
                    "property_house_number": parts[7],
                    "property_street_name": parts[8],
                    "property_locality": parts[9],
                    "property_post_code": parts[10],
                    "area": parts[11],
                    "area_type": area_type,
                    "contract_date": parts[13],
                    "settlement_date": parts[14],
                    "purchase_price": parts[15],
                    "zone_code": parts[16],
                    "zone_name": CODE_TO_ZONE.get(parts[16].strip(), ""),
                    "nature_property": nature_property,
                    "primary_purpose": parts[18],
                    "strata_number": parts[19],
                    "component_code": parts[20],
                    "sale_code": parts[21],
                    # Changed from '% Interest Sale' for valid identifier
                    "interest_sale": parts[22],
                    "dealing_number": parts[23],
                    "property_description": [],  # List to hold multiple C records
                    "purchaser_vendor": [],  # List to hold multiple D records, changed from 'Purchaser – Vendor'
                    "dimensions": "",
                    "filetype": "sales",
                }
                data["SALES"].append(sales_index[key])
            elif record_type == "C" and key in sales_index:
                sales_index[key]["property_description"].append(parts[5])
            elif record_type == "D" and key in sales_index:
                purchaser_vendor = {"P": "Purchaser", "V": "Vendor"}.get(
                    parts[5], parts[5]
                )
                sales_index[key]["purchaser_vendor"].append(purchaser_vendor)

    counter["records"] = sum(counter.values())
    validate = {k.split("_")[1]: int(v) for k, v in data["FOOTER"].items()}
    # assert all(counter[k] == validate[k] for k in validate), (file_path, counter, validate)
    return data


def handle_path(path):
    try:
        if "ARCHIVE_SALES" in path.name:
            res = parse_1990_file(path)
            res = res["SALES"]
        elif "SALES_DATA_NNME" in path.name:
            res = parse_sales_data_file(path)
            res = res["SALES"]
        else:
            res = []
        return res
    except:
        print(file)
        raise


def load_all_data(base):
    paths = list(Path(base).glob("*.DAT"))
    tracker = progress_tracker(len(paths), "Parsing")
    for path in paths:
        res = handle_path(path)
        for record in res:
            yield record
        tracker(path.stat().st_size)

def main():
    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Fetching sales data (for {when})")

    url = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
    headers = {
        "User-Agent": "Mozilla/5.0",
    }
    links = fetch_sales_data(url, headers)
    setup_directories()

    update_progress = progress_tracker(len(links))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(download_file, link[0], "extracted", update_progress)
            for link in links
        ]
        for future in futures:
            future.result()

    process_downloaded_files("extracted")
    shutil.rmtree("extracted")
    print(f"Fetched on: {when}")


if __name__ == "__main__":
    keyset = set()
    from pprint import pprint
    for record in load_all_data("test/data/"):
        keyset.add(tuple(sorted(record.keys())))
        if len(keyset) > 1:
            pprint(keyset)
            break
