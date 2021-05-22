#!/bin/bash
set -Eeuo pipefail
shopt -s globstar

when=$(date)
base="https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
echo "Fetching sales data (for %s)" "$when"
curl $base  \
| grep -E -o 'href="[^"]+(zip|pdf)"' \
| cut -f2 -d'"' \
| sort \
| head -n 10 \
| wget --continue -i -

mkdir -p pdfs
mkdir -p data
mkdir -p extracted

mv -f ./*.pdf pdfs/

mv -f ./*.zip extracted/

for name in ./extracted/**/*.zip; do
	echo "extracting: $name"
	unzip -quo "$name" -d extracted || true
	rm "$name"
done

for name in ./extracted/**/*.zip; do
	echo "extracting: $name"
	unzip -quo "$name" -d extracted || true
	rm "$name"
done

find extracted -name "*.DAT" -exec mv -f {} data/ \;

# rm -rf extracted

printf "Fetched on: %s\n" "$when" > when.txt

