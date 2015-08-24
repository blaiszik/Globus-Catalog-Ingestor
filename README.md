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
* Edit config.json and metadata_map.json

```
python ingest.py -c 'replace with catalog_name or ID'
```
