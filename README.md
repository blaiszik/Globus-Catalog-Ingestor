# Globus-Catalog-Ingestor

## OBTAIN GLOBUS CREDENTIALS
* https://www.globus.org/SignUp
* You will input this once when you first run this script



## INSTALL REQUIREMENTS
### Install Globus Catalog-client 
Details at (https://github.com/globusonline/catalog-client)

```
git clone https://github.com/globusonline/catalog-client
cd catalog-client
python setup.py install --user
```

### Clone this Repo
```
git clone https://github.com/blaiszik/Globus-Catalog-Ingestor.git
cd Globus-Catalog-Ingestor
```
* Edit config.json
  * "catalog_id": ID of the catalog you wish to push data into
  * "catalog_aliases": If you prefer to work with a catalog by name, add the numeric aliases here
  * "endpoint": Specify the endpoint location for your data
  * "path": Specify the path to your data
  * "files": Specify the location of the files ot parse (relative to the script)
  * "rw_users": Indicate which users should be granted read and write privileges
  * "r_users": Indicate which users should be granted read-only privileges


Example config.json
```json
{
    "catalog_id" : 137,
    "catalog_aliases" : { "ingestor suresh":137,
                          "other catalog":15},
    "endpoint" : "globus://s8idiuser#snow",
    "path": "/path/to/data",
    "files": ["B001_Eiger_silica150nm_water_test_Fq1_0001-20000.hdf",
              "B001_Eiger_silica150nm_water_test_Fq1_0001-20001.hdf"],
    "rw_users" : ["s8idiuser", "sureshn", "blaiszik"],
    "r_users" : ["bfrosik", "blaiszik"]
}
```

* Edit metadata_map.json (optional)

### Command line options
```
-c specify the catalog name (string) or ID (int) to ingest data into  (this overrrides config.json catalog_id entry)
-f specify a filename to be ingested (this overrrides config.json files entry)
-x suppress script console output
```

# Examples

## Using config.json (as above example)
```sh
python ingest.py
```

## Overriding config.json
Point the ingestor at a catalog and a file for ingesting
```sh
python ingest.py -c 'replace with catalog_name or ID' -f 'filename.hdf'
```





