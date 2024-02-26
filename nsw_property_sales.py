#!/usr/bin/env python3
import argparse
import csv
import os
import re
import shutil
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib import request, error

BASE_URL = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"

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

COLUMNS = (
    "district_code",
    "district_name",
    "property_id",
    "file_datetime",
    "property_name",
    "property_unit_number",
    "property_house_number",
    "property_street_name",
    "property_locality",
    "property_post_code",
    "area",
    "area_type",
    "contract_date",
    "settlement_date",
    "purchase_price",
    "zone_code",
    "zone_name",
    "nature_property",
    "primary_purpose",
    "strata_number",
    "component_code",
    "sale_code",
    "interest_sale",
    "dealing_number",
    "property_description",
    "purchaser_vendor",
    "dimensions",
    "filetype",
)

MANIFEST = []


def progress_tracker(total=None, operation="Working"):
    progress = [0]
    bytes_downloaded = [0]
    start_time = [time.time()]

    def update_progress(_bytes=0, step=1):
        progress[0] += step
        elapsed_time = time.time() - start_time[0]
        if total:
            percentage = (progress[0] / total) * 100
            percentage = f"({percentage:.2f}%)"
            _total = f"/{total}"
        else:
            _total = ""
            percentage = ""
        if _bytes is not None:
            bytes_downloaded[0] += _bytes
            throughput = (bytes_downloaded[0] / 1024 / 1024) / elapsed_time
            throughput = f" - {throughput:.2f} MiB/s"
            btotal = bytes_downloaded[0] / 1024 / 1024
            if btotal > 1024:
                btotal = f" - {btotal/1024:.2f}GiB"
            else:
                btotal = f" - {btotal:.1f}MiB"

        else:
            throughput = ""
            btotal = ""

        outstr = f"\r{operation}: {progress[0]}{_total} files {percentage}{throughput}{btotal}"
        print(
            outstr.ljust(80),
            end="",
            flush=True,
        )
        if total and progress[0] == total:
            print()

    return update_progress


def fetch_sales_data(url, headers):
    print("Searching for files.")
    req = request.Request(url, headers=headers)
    with request.urlopen(req) as response:
        html = response.read().decode("utf-8")
    return re.findall('href="([^"]+(zip|pdf))"', html)


def download_file(url, directory, progress_update):
    try:
        file_name = url.split("/")[-1]
        file_path = os.path.join(directory, file_name)

        with request.urlopen(url) as response:
            # Get the total file size from headers if available
            total_size = response.getheader("Content-Length")
            if total_size is not None:
                total_size = int(total_size)
            else:
                total_size = 0  # Unknown size

            downloaded_size = 0
            chunk_size = 1024 * 1024  # 1 MB per chunk

            with open(file_path, "wb") as out_file:
                while True:
                    data = response.read(chunk_size)
                    if not data:
                        break
                    out_file.write(data)
                    downloaded_size += len(data)
                    progress_update(downloaded_size, step=0)
            progress_update(0, step=1)
            MANIFEST.append(file_path)

        return file_path, downloaded_size
    except error.HTTPError as e:
        print(f"HTTP Error: {e.code} {e.reason} {url}")
    except error.URLError as e:
        print(f"URL Error: {e.reason} {url}")
    return None, 0


def fetch_data(download_path, pdf_path):
    headers = {
        "User-Agent": "Mozilla/5.0",
    }
    links = fetch_sales_data(BASE_URL, headers)
    tracker = progress_tracker(len(links), "Downloading")
    futures = []
    with ThreadPoolExecutor() as executor:
        for link, fkind in links:
            if fkind == "pdf":
                out_path = pdf_path
            elif fkind == "zip":
                out_path = download_path
            else:
                print("Unknown file type:", fkind)
                continue
            futures.append(executor.submit(download_file, link, out_path, tracker))
        for future in futures:
            future.result()


def extract_zip(file_path, target_path):
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(target_path)
        os.remove(file_path)
    except zipfile.BadZipFile:
        print(f"Failed to extract {file_path}, not a zip file.")
        raise


def process_downloaded_files(extracted_path, data_path):
    tracker = progress_tracker(None, "Extracting")
    zip_found = True
    while zip_found:
        zip_found = False
        for root, _, files in os.walk(extracted_path):
            for file in files:
                src_path = os.path.join(root, file)
                if file.endswith(".DAT") or file.endswith(".zip"):
                    dst_dir = data_path if file.endswith(".DAT") else extracted_path
                    index = 0
                    base, extension = os.path.splitext(file)
                    dst_path = os.path.join(dst_dir, f"{base}_{index}{extension}")
                    while os.path.exists(dst_path):
                        dst_path = os.path.join(dst_dir, f"{base}_{index}{extension}")
                        index += 1
                    shutil.move(src_path, dst_path)
                    MANIFEST.append(dst_path)
                    tracker(Path(dst_path).stat().st_size)
                    if file.endswith(".zip"):
                        extract_zip(dst_path, extracted_path)
                        zip_found = True
    print(flush=True)


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
                if len(data["SALES"]) > 0:
                    last = data["SALES"][-1]
                    last_desc = last["property_description"]
                    if isinstance(last_desc, list):
                        last["property_description"] = "".join(last_desc)

                    last_perch = last["purchaser_vendor"]
                    if isinstance(last_perch, list):
                        last["purchaser_vendor"] = ", ".join(last_perch)

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
    # validate = {k.split("_")[1]: int(v) for k, v in data["FOOTER"].items()}
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
        print("Failed on:", path)
        raise


def data_to_csv(base, out_path):
    paths = list(Path(base).glob("*.DAT"))
    tracker = progress_tracker(len(paths), "Parsing")
    with open(out_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for path in paths:
            res = handle_path(path)
            for record in res:
                writer.writerow(record)
            tracker(path.stat().st_size)
    MANIFEST.append(out_path)


def write_manifest(manifest_path, when):
    MANIFEST.append(manifest_path)
    with open(manifest_path, "w") as f:
        f.write(f"NSW Land Data Manifest (as of {when})\n\n")
        for line in MANIFEST:
            f.write(str(line) + "\n")


def main():
    start = time.time()
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--download_path",
        type=Path,
        default="./downloads",
        help="Path where downloads will be stored",
    )
    parser.add_argument(
        "--data_path",
        type=Path,
        default="./extracted",
        help="Path where extracted data will be stored",
    )
    parser.add_argument(
        "--csv_path",
        type=Path,
        default="./land_value.csv",
        help="Path to output CSV file",
    )
    parser.add_argument(
        "--pdf_path", type=Path, default="./pdfs", help="Path to PDF files"
    )
    parser.add_argument(
        "--manifest_file",
        type=Path,
        default="./manifest.txt",
        help="A manifest of all files donwloaded, and parsed.",
    )
    parser.add_argument(
        "--keep_raw_files",
        action="store_true",
        default=False,
        help="Keep the raw data directories",
    )

    args = parser.parse_args()
    args.download_path.mkdir(parents=True, exist_ok=True)
    args.data_path.mkdir(parents=True, exist_ok=True)
    args.pdf_path.mkdir(parents=True, exist_ok=True)

    try:
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"NSW Bulk property sales information downloader. (as of {when})")
        print(f"Fetching sales data (to '{args.download_path}').")
        fetch_data(args.download_path, args.pdf_path)
        print(f"Extracting data files. (to '{args.data_path}')")
        process_downloaded_files(args.download_path, args.data_path)
        print(f"Converting to CSV. (to '{args.csv_path}')")
        data_to_csv(args.data_path, args.csv_path)
        print(f"Writing manifest. (to '{args.manifest_file}')")
        write_manifest(args.manifest_file, when)
    finally:
        if not args.keep_raw_files:
            print(f"Removing raw files. ('{args.download_path}', '{args.data_path}')")
            shutil.rmtree(args.download_path)
            shutil.rmtree(args.data_path)
    duration = time.time() - start
    print(f"Done. (in {duration:.2f}s)")


if __name__ == "__main__":
    main()
