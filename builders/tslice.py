import os
import sys
from collections import OrderedDict

import pandas as pd
import yaml
import click
import dask
import numpy as np
import requests
import json
import netCDF4 as nc
from core import Builder, extract_attr_with_regex, get_asset_list, reverse_filename_format
from dask.diagnostics import ProgressBar


def write_json(cols, csv_filepath, yaml_path):
    """
    Write a json file based on the cols in the df.  The
    file name will match the csv file name to keep consistancy.
    """
    json_filepath = '.'.join(csv_filepath.split(".")[:-2])+".json"
    #create the json dictionary and start with some "defaults"
    d = {
        "esmcat_version": "0.1.0",
        "id": "my_data",
        "description": "New catalog for data pointed to within the root dir "+yaml_path,
        "catalog_file": csv_filepath,
        "attributes": [],
        "assets": {
                  "column_name": "path",
                  "format": "netcdf"
                  },
        "aggregation_control": {
           "variable_column_name": "time_range",
           "groupby_attrs": ["time_range"],
           "aggregations":[
                           {
                           "type": "union",
                           "attribute_name": "time_range"
                           },
                           {
                           "type": "join_existing",
                           "attribute_name": "time_range",
                           "options": {
                                      "dim": "time",
                                      "coords": "minimal",
                                      "compat": "override"
                                      }
                           },
           ] 
        }                         
    }
    # add a section for each column
    for col in cols:
        c = {
            "column_name": col,
            "vocabulary": ""
            }
        d["attributes"].append(c)

    od=OrderedDict(d.items())
    #write the json file
    with open(json_filepath,'w') as f:
        json.dump(od,f,indent=4)


def verify(input_yaml):
    # verify that we're working with a dictionary
    if not isinstance(input_yaml,dict):
        print("ERROR: The experiment/dataset top level is not a dictionary. Make sure you follow the correct format.")
        return False
    for dataset in input_yaml.keys():
        # check to see if there is a data_sources key for each dataset
        if 'data_sources' not in input_yaml[dataset].keys():
            print("ERROR: Each experiment/dataset must have the key 'data_sources'. Verify "+dataset+" contains this key.")
            return False
        # verify that we're working with a list at this level
        if not isinstance(input_yaml[dataset]['data_sources'],list):
            print("ERROR: The data_sources are not in a list.  Make sure you follow the correct format.")
            return False
        for stream_info in input_yaml[dataset]['data_sources']:
            # check to make sure that there's a 'glob_string' key for each data_source
            if 'glob_string' not in stream_info.keys():
                print("ERROR: Each data_source must contain a 'glob_string' key.  Verify that all data_sources under "+dataset+" contain a 'glob_string' key.")
                return False
    return True


def common_parser(filepath, local_attrs, glob_attrs):
    """
    gather the file parts for the df
    """
    basename = os.path.basename(filepath)
    fileparts = {}
    fileparts['path'] = filepath

    try:
        fileparts['variable'] = []
        # open file
        d = nc.Dataset(filepath,'r')
        # find what the time (unlimited) dimension is
        dims = list(dict(d.dimensions).keys())
        look_for_unlim=[d.dimensions[dim].isunlimited() for dim in dims]
        unlim=[i for i, x in enumerate(look_for_unlim) if x]
        if len(unlim)>0:
            unlim_dim=dims[unlim[0]]
        if unlim_dim in d.variables.keys():
            fileparts['time_range'] = str(d[unlim_dim][0])+'-'+str(d[unlim_dim][-1])
        # loop through all variables
        for v in d.variables.keys():
            # add all variables that are not coordinates to the catalog
            if v not in dims:
                fileparts['variable'].append(v)
        # add the keys that are common to all files in the dataset
        for gv in glob_attrs.keys():
            fileparts[gv] = glob_attrs[gv]
        # add the keys that are common just to the particular glob string
        fileparts.update(local_attrs[filepath])
    except Exception:
        pass
    return fileparts

def build_df(
    yaml_path,
    depth=1,
    columns=None,
    exclude_patterns=[],
):
    parser = common_parser
    
    with open(yaml_path,'r') as f:
        input_yaml = yaml.safe_load(f)  
    # verify the format is correct
    if verify(input_yaml):
        # loop over datasets
        df_parts = []
        entries = defaultdict(dict)
        for dataset in input_yaml.keys():
            ds_globals = {}
            # get a list of keys that are common to all files in the dataset
            for g in input_yaml[dataset].keys():
                if 'data_sources' not in g and 'ensemble' not in g:
                    ds_globals[g] = input_yaml[dataset][g]
            # loop over ensemble members, if they exist
            if 'ensemble' in input_yaml[dataset].keys():
                for member in input_yaml[dataset]['ensemble']:
                    glob_string = member.pop('glob_string')
                    filelist = get_asset_list(glob_string, depth=0) 
                    for f in filelist:
                        entries[f] = member
            # loop over all of the data_sources for the dataset, create a dataframe
            # for each data_source, append that dataframe to a list that will contain
            # the full dataframe (or catalog) based on everything in the yaml file.
            #for stream_info in input_yaml[dataset]['data_sources']:
            for stream_info in input_yaml[dataset]['data_sources']:
                filelist = get_asset_list(stream_info['glob_string'], depth=0)
                stream_info.pop('glob_string')
                for f in filelist:
                    if f in entries.keys():
                        entries[f].update(stream_info)
                    else:
                        entries[f] = stream_info
                if columns is None:
                    columns = []
                b = Builder(columns, exclude_patterns)
                df_parts.append(b(filelist, parser, d=entries, g=ds_globals))
                # create the combined dataframe from all of the data_sources and datasets from
                # the yaml file
                df = pd.concat(df_parts,sort=False)
        print(df)
        return df.sort_values(by=['path'])
    else:
        print("ERROR: yaml file is not formatted correctly.  See above errors for more information.")
        sys.exit(-1)


@click.command()
@click.option(
    '--yaml-path', type=click.Path(exists=True), help='The full path of the input yaml file.'
)
@click.option(
    '-d',
    '--depth',
    default=4,
    type=int,
    show_default=True,
    help='Recursion depth. Recursively walk yaml_path to a specified depth',
)
@click.option('--csv-filepath', type=str, help='File path to use when saving the built catalog')
def cli(yaml_path, depth, csv_filepath):


    if csv_filepath is None:
        raise ValueError("Please provide csv-filepath. e.g.: './myCatalog.csv.gz'")

    df = build_df(yaml_path, depth=depth)

    write_json(df.columns, csv_filepath, yaml_path)

    df.to_csv(csv_filepath, compression='gzip', index=False)


if __name__ == '__main__':
    cli()
