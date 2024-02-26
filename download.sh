#!/bin/bash
set -Eeuo pipefail

when=$(date)
echo "Fetching sales data (for %s)" "$when"
curl -Lkv 'https://valuation.property.nsw.gov.au/embed/propertySalesInformation' -H 'User-Agent: Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Upgrade-Insecure-Requests: 1' -H 'Sec-GPC: 1' -H 'Cache-Control: max-age=0' \
| grep -E -o 'href="[^"]+(zip|pdf)"' \
| cut -f2 -d'"' \
| sort \
| wget --continue -i -

mkdir -p pdfs
mkdir -p data
mkdir -p extracted

mv -f ./*.pdf pdfs/

mv -f ./*.zip extracted/

for name in ./extracted/*.zip; do
    echo "extracting: $name"
    unzip -quo "$name" -d extracted || true
    rm "$name"
done

for name in ./extracted/*.zip; do
    echo "extracting: $name"
    unzip -quo "$name" -d extracted || true
    rm "$name"
done

echo "Moving data to final location"
find extracted -name "*.DAT" -exec mv -f {} data/ \;

rm -rf extracted

printf "Fetched on: %s\n" "$when"

