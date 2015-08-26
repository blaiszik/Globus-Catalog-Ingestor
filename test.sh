#!/bin/sh

#Edit these before testing
test_catalog=137
test_dataset=253

echo "=== Testing with no arguments -- config.json only ==="
python ingest.py
echo '\n\n'

echo "=== Testing ingest into a catalog by name ==="
python ingest.py -c 'ingestor suresh'
echo '\n\n'

echo "=== Testing ingest into a catalog by ID ==="
python ingest.py -c $test_catalog
echo '\n\n'

echo "=== Testing ingest into a dataset ==="
python ingest.py -c $test_catalog -d $test_dataset 
echo '\n\n'