import os

import click
import dask
import xarray as xr
import numpy as np
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
        d = xr.open_dataset(filepath)
        for v in d.variables.keys():
            fileparts['variable'].append(v)
        d.close()
        print(filepath+"   "+str(len(fileparts['variable'])))

        parent = basename.split(".")
        fileparts['case_name'] = '.'.join(parent[:-3])
        fileparts['ensemble_member'] = "001"
        fileparts['model'] = parent[-4]
        fileparts['stream'] = parent[-3]
        fileparts['time_range'] = parent[-2]
        fileparts['realm'] = filepath.split("/")[-3]

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
            'case_name',
            'ensemble_member',
            'realm',
            'model',
            'stream',
            'variable',
            'time_range',
            'path', 
        ],
    }

    filelist = get_asset_list(root_path, depth=depth)
    data_type = str(data_type)
    if columns is None:
        columns = df_columns[data_type]
    b = Builder(columns, exclude_patterns)
    df = b(filelist, parsers[data_type])
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
