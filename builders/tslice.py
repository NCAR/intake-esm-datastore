import os
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

def common_parser(filepath, local_attrs, glob_attrs):
    """
    gather the file parts for the df
    """
    print("Passed info:")
    print(filepath)
    print(glob_attrs)
    print(local_attrs)
    print("---------------------")
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
        unlim_dim=dims[unlim[0]]
        fileparts['time_range'] = str(d[unlim_dim][0])+'-'+str(d[unlim_dim][-1])
        # loop through all variables
        for v in d.variables.keys():
            # test to see if this is a time varying field, if so, add to the catalog
            if unlim_dim in d.variables[v].dimensions:
                fileparts['variable'].append(v)
        for gv in glob_attrs.keys():
            fileparts[gv] = glob_attrs[gv]
        for lv in local_attrs.keys():
            if 'glob_string' not in v:
                fileparts[lv] = local_attrs[lv]
        # add all global attributes to the db column list.  this counts on enough information 
        # being there to create enough serachable categories.
#        for attr in d.ncattrs():
#            fileparts[attr] = d.getncattr(attr)
#        d.close()
    except Exception:
        pass
    print(fileparts)
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
    # loop over datasets
    df_parts = []
    for dataset in input_yaml.keys():
        ds_globals = {}
        for g in input_yaml[dataset].keys():
            if 'data_sources' not in g:
                ds_globals[g] = input_yaml[dataset][g]
        for stream_info in input_yaml[dataset]['data_sources']:
            filelist = get_asset_list(stream_info['glob_string'], depth=0)
            if columns is None:
                columns = []
            b = Builder(columns, exclude_patterns)
            df_parts.append(b(filelist, parser, d=stream_info, g=ds_globals))
            #print('-------------------------')
            #print(df)
            #print('-------------------------')
    df = pd.concat(df_parts,sort=False) 
    return df.sort_values(by=['path'])


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
