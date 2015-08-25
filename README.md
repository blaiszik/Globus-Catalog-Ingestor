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
* Edit config.json !! Do not commit this back to the git repo!!
  * Input any required catalog aliases
  * Create list of files to be ingested (can optionally be specified on the command line)
  * Add login details if preferred 
  * Input default catalog ID if preferred
  * Add read and read_write privileges
  * Specify data endpoint location

Example config.json
```json
{
    "base_url" : "https://catalog-alpha.globuscs.info/service/dataset",
    "endpoint" : "globus://s8idiuser#snow",
    "username" : "",
    "password" : "",
    "token" : "/path/to/.ssh/token.txt-replace",
    "files": ["B001_Eiger_silica150nm_water_test_Fq1_0001-20000.hdf"],
    "path_prefix" : "",
    "catalog_id" : 137,
    "catalog_aliases" : {"ingestor suresh":137},
    "rw_users" : ["s8idiuser", "sureshn", "blaiszik"],
    "r_users" : ["bfrosik", "blaiszik"]
}
```

* and metadata_map.json

### Command line options
```
-c specify the catalog name (string) or ID (int) to ingest data into  (this overrrides config.json catalog_id entry)
-f specify a filename to be ingested (this overrrides config.json files entry)
-x suppress script console output
```

# Examples

```
python ingest.py -c 'replace with catalog_name or ID' -f 'filename.hdf'
```





