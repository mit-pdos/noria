#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
URL="http://jmob.ow2.org/tpcw/generate.tar.gz"

mkdir -p generate
mkdir -p data

# download
wget ${URL} -O generate/generate.tar.gz
tar -xzf generate/generate.tar.gz

# patch & build generator
cd generate
echo "char *getRandString(char *str, int l, int h);" >> tpcw-spec.h
make clean && make all

# generate tables
echo "Generating countries..."
./tpcw -t country >> ../data/countries.tsv
echo "Generating authors..."
./tpcw -t author >> ../data/authors.tsv
echo "Generating customers..."
./tpcw -t customer >> ../data/customer.tsv
echo "Generating addresses..."
./tpcw -t address >> ../data/address.tsv
echo "Generating orders..."
./tpcw -t orders -p ../data >> ../data/orders.tsv
