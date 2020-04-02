import os

import click
import dask
import numpy as np
import netCDF4 as nc
import requests
from core import Builder, extract_attr_with_regex, get_asset_list, reverse_filename_format
from dask.diagnostics import ProgressBar


def cesm_parser(filepath):
    """
    Extract attributes of a file using information from CMI6 DRS.
    References
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
        unlim_dim=dims[unlim[0]]
        # loop through all variables
        for v in d.variables.keys():
            # test to see if this is a time varying field, if so, add to the catalog
            if unlim_dim in d.variables[v].dimensions:
                fileparts['variable'].append(v)
        # add all global attributes to the db column list.  this counts on enough information 
        # being there to create enough serachable categories.
        for attr in d.ncattrs():
            fileparts[attr] = d.getncattr(attr)
        d.close()

    except Exception:
        pass

    return fileparts


def build_df(
    root_path,
    data_type,
    depth=1,
    columns=None,
    exclude_patterns=['*/files/*', '*/latest/*'],
):
    parsers = {'2': cesm_parser}
    df_columns = {
        '2': [
        ],
    }

    filelist = get_asset_list(root_path, depth=depth)
    data_type = str(data_type)
    if columns is None:
        columns = df_columns[data_type]
    b = Builder(columns, exclude_patterns)
    df = b(filelist, parsers[data_type])
    #print('-------------------------')
    #print(df)
    #print('-------------------------')
    return df.sort_values(by=['path'])


@click.command()
@click.option(
    '--root-path', type=click.Path(exists=True), help='Root path of the CMIP project output.'
)
@click.option(
    '-d',
    '--depth',
    default=4,
    type=int,
    show_default=True,
    help='Recursion depth. Recursively walk root_path to a specified depth',
)
@click.option('-v', '--data_type', type=int, help='data_type can be 2(time series)')
@click.option('--csv-filepath', type=str, help='File path to use when saving the built catalog')
def cli(root_path, depth, data_type, csv_filepath):

    if data_type not in set([2]):
        raise ValueError(
            f'data_type = {data_type} is not valid. Valid options include: 2.'
        )

    if csv_filepath is None:
        raise ValueError("Please provide csv-filepath. e.g.: './myCatalog.csv.gz'")

    df = build_df(root_path, data_type, depth=depth)

    df.to_csv(csv_filepath, compression='gzip', index=False)


if __name__ == '__main__':
    cli()
