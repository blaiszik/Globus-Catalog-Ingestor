# DataFinder.py    
# Copyright (c) 2013-2014, UChicago Argonne, LLC
# See LICENSE file.
"""
    Pushes data to Globus Catalog using Ingestor class.
    Reads from the commandline the following:
        input file -- Required (no tag)
        metadata file -- Optional text file (-x tag)
        Catalog ID -- Required (no tag, either name or ID can be used)
        Dataset ID -- Optional (-d tag, name or ID used, must be in config file, used only if pushing data as member annotations)

    Gets some (or all) datasets and related attributes from an HDF5 file. Takes in an HDF5 file as input and an optional textfile containing individual metadata paths and custom tag names for pushing to the catalog or simply outputting to the terminal.

"""
import argparse
import sys
import h5py
import os
import json

from globusonline.catalog.client.goauth import get_access_token
from globusonline.catalog.client.dataset_client import DatasetClient
from globusonline.catalog.client.rest_client import RestClientError
from globusonline.catalog.client.catalog_wrapper import *


config_file = "./config.json"
metadata_map_file = "./metadata_map.json"
config = {}
metadata_map = {}

#Load the basic configurations for the ingestor
with open(config_file) as data_file:    
    config = json.load(data_file)

#Load the metadata mappings
with open(metadata_map_file) as data_file:    
    metadata_map = json.load(data_file)

# Only {'text', 'int8', 'float8', 'boolean', 'timestamptz', 'date'} are allowed as 'value_type' due to the schema used.
maps = {'|S1024':'text', 'int8':'int8', 'int16':'int8', 'int32':'int8', 'float64':'float8',
        'float32':'float8', 'uint16':'int8', 'uint64':'int8', '|S24':'text', '|S4':'text', 
        '|S7':'text', '|S10':'text', '|S8':'text', '|S5':'text'}


def get_catalog_type(hdf5_type):
        """Converts the data type from the HDF5 data type to a type recognized by the API.
           (Uses some common prefixes and the 'maps' dictionary located near the top of the script.)"""

        # Check if the type is object, it is most likely to be a string
        if hdf5_type.kind == 'O':
            dt = h5py.check_dtype(vlen=hdf5_type)
            if hasattr(dt, '__name__'):
                if dt.__name__ == 'str':
                   return 'text'

        old_type = str(hdf5_type)
        if old_type[:2] == '|S':
            return 'text'
        elif old_type[:3] == 'int':
            return 'int8'
        elif old_type[:4] == 'uint':
            return 'int8'
        elif old_type[:5] == 'float':
            return 'float8'
        elif old_type in maps:
            return maps[old_type]
        else:
            return None

# Get a list of the annottions that are not in the current catalog
def check_annotations(annotations):
    global client, catalog_id
    catalog_annotation_list = []
    result = client.get_annotation_defs(catalog_id)
    for ann in result.body:
        catalog_annotation_list.append(ann['name'])
    annotation_diff = list(set(annotations)-set(catalog_annotation_list))
    return annotation_diff

def add_users(catalog_id, dataset_id):
    """Adds users for read and write access when a new dataset is created."""
    global client
    access_rules = []
    tmp_access_rule = {}
    for new_user in config.get('rw_users'):
        tmp_access_rule = {'permission': 'rw', 'principal_type':'user', 'principal':new_user}
        access_rules.append(tmp_access_rule)
    for new_user in config.get('r_users'):
        tmp_access_rule = {'permission': 'r', 'principal_type':'user', 'principal':new_user}
        access_rules.append(tmp_access_rule)
    print_d(access_rules)
    response = client.add_dataset_acl(catalog_id, dataset_id, access_rules)
    print_d(response)

def visit_hdf(hdf_file):
    """ Visit the whole tree to get list of all dataset paths
    """
    dataset_list = []
    def func(name, dset):
        if isinstance(dset, h5py.Dataset):
            dataset_list.append(dset.name)

    hdf_file.visititems(func)
    return dataset_list

def ingest_as_members(hdf_file, dataset_id):
 # """Prepares the dataset to be pushed to the API when pushing the metadata as tags on a dataset."""
    global client, catalog_id, config
    hdf_datasets = {}  
    annotation_to_add_values = {}
    annotation_to_add_types = {} 

    dataset_names = visit_hdf(hdf_file)
    add_users(catalog_id, dataset_id)

    for name in dataset_names:
        data_string = str(f[name][()]).replace('[', '').replace(']', '')
        hdf_datasets[name] = {'tag':str(f[name][()]).replace('[', '').replace(']', '')}

        if name in metadata_map:
            print_d("Mapping to metadata file")
            print_d(type(metadata_map))
            print_d(metadata_map.keys())
            print_d(metadata_map)
            hdf_datasets[name]['tag_short'] = metadata_map[name]
        else:
            tmp = str(f[name].name).split('/')
            if (len(tmp) > 1):
                hdf_datasets[name]['tag_short'] = "%s_%s"%(tmp[len(tmp)-2], tmp[len(tmp)-1])
            else:
                hdf_datasets[name]['tag_short'] = str(f[name].name)[str(f[name].name).rfind('/') + 1:]
           
        hdf_datasets[name]['tag'] = str(f[name][()]).replace('[', '').replace(']', '')
        hdf_datasets[name]['type'] = get_catalog_type(f[name].dtype)
        hdf_datasets[name]['shape'] = f[name].shape

    for dataset in hdf_datasets:
        tmp_annotation = {}
        if hdf_datasets[dataset]['shape'] == (1,1) or hdf_datasets[dataset]['shape'] == ():
            tmp_annotation = {'tag':hdf_datasets[dataset]['tag'], 'tag_name':hdf_datasets[dataset]['tag_short']}
            annotation_to_add_values[hdf_datasets[dataset]['tag_short']] = hdf_datasets[dataset]['tag']
            annotation_to_add_types[hdf_datasets[dataset]['tag_short']] = hdf_datasets[dataset]['type']

    annotation_diff = check_annotations(annotation_to_add_values.keys())
    print_d(annotation_diff)

    #Create necessary annotations (multi call) - only required once
    for annotation in annotation_diff:
        print_d("Added annotation "+annotation)
        response = client.create_annotation_def \
                (catalog_id=catalog_id, annotation_name=annotation,
                 value_type=annotation_to_add_types[annotation],   multivalued=False)
    print_d("Finished adding annotations")


    data_uri = "%s/%s/%s"%(config['endpoint'], config['path'],hdf_file.filename)
    new_member = {"data_type":"file", "data_uri":data_uri}
    #Create a member
    response = client.create_members(catalog_id,dataset_id,new_member)
    member_id = response.body['id']
    print_d(response)

    #Add annotations (bulk insert)
    response = client.add_member_annotations \
                (catalog_id, dataset_id, member_id, annotation_to_add_values)
    print_d("Added annotations in bulk")

    if output:
        print "====="
        print "Successfully ingested from %s as members \n into (Catalog, Dataset, Member) (%s, %s, %s)"%(hdf_file.filename, catalog_id, dataset_id, member_id)
        print "====="

    f.close()




def ingest_as_datasets(hdf_file):
    # """Prepares the dataset to be pushed to the API when pushing the metadata as tags on a dataset."""
    global client, catalog_id
    hdf_datasets = {}  
    annotation_to_add_values = {}
    annotation_to_add_types = {} 

    dataset_names = visit_hdf(hdf_file)
    #print dataset_names
    new_dataset = {"name":hdf_file.filename}

    #Create the dataset
    response = client.create_dataset(catalog_id, new_dataset)
    # TODO; Check the response code
    dataset_id = response.body['id']
    add_users(catalog_id, dataset_id)
    #print new_dataset

    for name in dataset_names:
        data_string = str(f[name][()]).replace('[', '').replace(']', '')
        hdf_datasets[name] = {'tag':str(f[name][()]).replace('[', '').replace(']', '')}

        if name in metadata_map:
            print_d("Mapping to metadata file")
            print_d(type(metadata_map))
            print_d(metadata_map.keys())
            print_d(metadata_map)
            hdf_datasets[name]['tag_short'] = metadata_map[name]
        else:
            tmp = str(f[name].name).split('/')
            if (len(tmp) > 1):
                hdf_datasets[name]['tag_short'] = "%s_%s"%(tmp[len(tmp)-2], tmp[len(tmp)-1])
            else:
                hdf_datasets[name]['tag_short'] = str(f[name].name)[str(f[name].name).rfind('/') + 1:]
           
        hdf_datasets[name]['tag'] = str(f[name][()]).replace('[', '').replace(']', '')
        hdf_datasets[name]['type'] = get_catalog_type(f[name].dtype)
        hdf_datasets[name]['shape'] = f[name].shape

    for dataset in hdf_datasets:
        tmp_annotation = {}
        if hdf_datasets[dataset]['shape'] == (1,1) or hdf_datasets[dataset]['shape'] == ():
            tmp_annotation = {'tag':hdf_datasets[dataset]['tag'], 'tag_name':hdf_datasets[dataset]['tag_short']}
            annotation_to_add_values[hdf_datasets[dataset]['tag_short']] = hdf_datasets[dataset]['tag']
            annotation_to_add_types[hdf_datasets[dataset]['tag_short']] = hdf_datasets[dataset]['type']

    annotation_diff = check_annotations(annotation_to_add_values.keys())
    print_d(annotation_diff)

    #Create necessary annotations (multi call) - only required once
    for annotation in annotation_diff:
        print_d("Added annotation "+annotation)
        response = client.create_annotation_def \
                (catalog_id=catalog_id, annotation_name=annotation,
                 value_type=annotation_to_add_types[annotation],   multivalued=False)
    print_d("Finished adding annotations")

    #Add annotations (bulk insert)
    response = client.add_dataset_annotations \
                (catalog_id, dataset_id, annotation_to_add_values)
    print_d("Added annotations in bulk")
    
    if output:
        print "====="
        print "Successfully ingested from %s as datasets \n into (Catalog, Dataset) (%s, %s)"%(hdf_file.filename, catalog_id, dataset_id)
        print "====="

    f.close()

def print_d(input):
    global debug
    if debug:
        print input
    else:
        pass

if __name__ == "__main__":
    # Store authentication data in a local file
    debug = False
    output = True
    ingest_into = "catalog"

    token_file = os.getenv('HOME','')+"/.ssh/gotoken.txt"
    wrap = CatalogWrapper(token_file=token_file)
    client = wrap.catalogClient

    catalog_id = config.get('catalog_id')
    dataset_id = config.get('dataset_id')
    cl_file = config.get('files')

    """Import an input file and optional metadata file.
    Initialize an object of type Ingestor to do this.
    Print the datasets in both the input file and also in the metadata file.
    See above for more detailed info and examples.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", metavar = "File", help = "The hdf5 file to push / output.", type=str)
    parser.add_argument("-c", metavar = "Catalog", help = "The ID of the catalog to push data into.", type=str)
    parser.add_argument("-d", metavar = "Dataset", help = "The ID of the dataset to push data into (optional)", type=str)
    parser.add_argument("-x", metavar = "Suppress", help = "Suppress output", type=bool)



    args = parser.parse_args()
    
    if args.f:
        cl_file = args.f
    if args.c:
        if type(args.c) is int:
            cl_catalog = args.c
        else:
            if args.c in config['catalog_aliases']:
                cl_catalog = config['catalog_aliases'][args.c]
            else:
                raise ValueError('Catalog name not found in aliases - check config file')
    if args.d:
        dataset_id = int(args.d)
        ingest_into = "dataset"

    if args.x is not None:
        output = not args.x
        print output
    print_d(config)

    # Ingest a list of files from config
    if type(cl_file) is list:
        for h5file in cl_file:
            print "Ingesting %s"%(h5file)
            f = h5py.File(h5file, 'r')
            if ingest_into == "catalog":
                ingest_as_datasets(f)
            elif ingest_into == "dataset":
                ingest_as_members(f, dataset_id)
    # Ingest a single file from command line input
    else:
        f = h5py.File(cl_file, 'r')
        ingest_as_datasets(f)



   





















