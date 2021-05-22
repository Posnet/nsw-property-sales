#!/bin/bash
when=$(date)
base="https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
echo "Fetching sales data (for %s)" "$when"
curl $base  \
| grep -E -o 'href="[^"]+zip"' \
| cut -f2 -d'"' \
| head -n 3 \
| wget --continue -i -

mkdir -p pdfs
mkdir -p data
mkdir -p extracted

mv *.pdf pdfs/

mv *.zip extracted/

for name in extracted/*.zip; do
	echo "extracting: $name"
	unzip -quo "$name" -d extracted
	# rm "$name"
done

for name in extracted/**/*.zip; do
	echo "extracting: $name"
	unzip -quo "$name" -d extracted
	# rm "$name"
done

for name in extracted/*.DAT; do
	echo "Moving: $name"
    mv -f $name data/
done

printf "Fetched on: %s\n" "$when" > when.txt

