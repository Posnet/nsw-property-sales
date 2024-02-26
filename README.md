# NSW Bulk property sales information downloader

`nsw_property_sales.py.py` is a self contained python script that will download all available data from the NSW government's property sales information website. 

The data is available at https://www.valuergeneral.nsw.gov.au/land-and-property-information/property-sales.

Use at your own risk, this script will download a lot of data and may take a long time to run. Make sure you have at least 2GB of free space on your hard drive before running this script.

For help just run `./nsw_property_sales.py -h`.

By default the script just deletes the raw files and keep only the final CSV.
To keep the raw files, pass the argument `--keep-raw-files`.

If you want to explore the data without using something like `pandas` I recommend either
https://www.visidata.org/install/ or https://github.com/BurntSushi/xsv


Fair License


Alec Posney `posnet@denialof.services` (c) 2024


Usage of the works is permitted provided that this instrument is retained with the works, so that any entity that uses the works is notified of this instrument.


DISCLAIMER: THE WORKS ARE WITHOUT WARRANTY.
