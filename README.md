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

* and metadata_map.json

### Command line options
-c specify the catalog name (string) or ID (int) to ingest data into
-f specify a filename to be ingested
-x suppress script console output

# Examples

```
python ingest.py -c 'replace with catalog_name or ID' -f 'filename.hdf'
```





