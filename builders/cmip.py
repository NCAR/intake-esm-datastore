import os

import click
import numpy as np
from core import Builder, extract_attr_with_regex, get_file_list, reverse_filename_format

cmip6_columns = [
    'activity_id',
    'institution_id',
    'source_id',
    'experiment_id',
    'member_id',
    'table_id',
    'variable_id',
    'grid_label',
    'dcpp_init_year',
    'version',
    'time_range',
    'path',
]
cmip5_columns = [
    'product_id',
    'institute',
    'model',
    'experiment',
    'frequency',
    'modeling_realm',
    'mip_table',
    'ensemble_member',
    'variable',
    'temporal_subset',
    'version',
    'path',
]
exclude_patterns = ['*/files/*', '*/latest/*']


def cmip6_parser(filepath):
    """
    Extract attributes of a file using information from CMI6 DRS.
    References

    CMIP6 DRS: http://goo.gl/v1drZl
    Controlled Vocabularies (CVs) for use in CMIP6: https://github.com/WCRP-CMIP/CMIP6_CVs
    Directory structure =

    <mip_era>/
        <activity_id>/
            <institution_id>/
                <source_id>/
                    <experiment_id>/
                        <member_id>/
                            <table_id>/
                                <variable_id>/
                                    <grid_label>/
                                        <version>
    file name = <variable_id>_<table_id>_<source_id>_<experiment_id >_<member_id>_<grid_label>[_<time_range>].nc For time-invariant fields, the last segment (time_range) above is omitted. Example when there is no sub-experiment: tas_Amon_GFDL-CM4_historical_r1i1p1f1_gn_196001-199912.nc Example with a sub-experiment: pr_day_CNRM-CM6-1_dcppA-hindcast_s1960-r2i1p1f1_gn_198001-198412.nc
    """
    basename = os.path.basename(filepath)
    filename_template = '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}_{time_range}.nc'

    gridspec_template = (
        '{variable_id}_{table_id}_{source_id}_{experiment_id}_{member_id}_{grid_label}.nc'
    )
    templates = [filename_template, gridspec_template]
    fileparts = reverse_filename_format(basename, templates=templates)
    try:
        parent = os.path.dirname(filepath).strip('/')
        parent_split = parent.split(f"/{fileparts['source_id']}/")
        part_1 = parent_split[0].strip('/').split('/')
        grid_label = parent.split(f"/{fileparts['variable_id']}/")[1].strip('/').split('/')[0]
        fileparts['grid_label'] = grid_label
        fileparts['activity_id'] = part_1[-2]
        fileparts['institution_id'] = part_1[-1]
        version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'
        version = extract_attr_with_regex(parent, regex=version_regex) or 'v0'
        fileparts['version'] = version
        fileparts['path'] = filepath
        if fileparts['member_id'].startswith('s'):
            fileparts['dcpp_init_year'] = float(fileparts['member_id'].split('-')[0][1:])
            fileparts['member_id'] = fileparts['member_id'].split('-')[-1]
        else:
            fileparts['dcpp_init_year'] = np.nan

    except Exception:
        pass

    return fileparts


def cmip5_parser(filepath):
    """ Extract attributes of a file using information from CMIP5 DRS.
    Notes
    -----
    Reference:
    - CMIP5 DRS: https://pcmdi.llnl.gov/mips/cmip5/docs/cmip5_data_reference_syntax.pdf?id=27
    """

    freq_regex = r'/3hr/|/6hr/|/day/|/fx/|/mon/|/monClim/|/subhr/|/yr/'
    realm_regex = r'aerosol|atmos|land|landIce|ocean|ocnBgchem|seaIce'
    version_regex = r'v\d{4}\d{2}\d{2}|v\d{1}'

    file_basename = os.path.basename(filepath)

    filename_template = (
        '{variable}_{mip_table}_{model}_{experiment}_{ensemble_member}_{temporal_subset}.nc'
    )
    gridspec_template = '{variable}_{mip_table}_{model}_{experiment}_{ensemble_member}.nc'

    templates = [filename_template, gridspec_template]
    fileparts = reverse_filename_format(file_basename, templates)
    frequency = extract_attr_with_regex(filepath, regex=freq_regex, strip_chars='/')
    realm = extract_attr_with_regex(filepath, regex=realm_regex)
    version = extract_attr_with_regex(filepath, regex=version_regex) or 'v0'
    fileparts['frequency'] = frequency
    fileparts['modeling_realm'] = realm
    fileparts['version'] = version
    fileparts['path'] = filepath
    try:
        part1, part2 = os.path.dirname(filepath).split(fileparts['experiment'])
        part1 = part1.strip('/').split('/')
        fileparts['institute'] = part1[-2]
        fileparts['product_id'] = part1[-3]
    except Exception:
        pass

    return fileparts


def pick_latest_version(df):
    grpby = list(set(df.columns.tolist()) - {'path', 'version'})
    groups = df.groupby(grpby)
    idx_to_remove = []
    for _, group in groups:
        if group.version.nunique() > 1:
            idx_to_remove.extend(
                group.sort_values(by=['version'], ascending=False).index[1:].values.tolist()
            )
    df = df.drop(index=idx_to_remove)
    return df


def build_cmip6(
    root_path,
    depth=3,
    columns=cmip6_columns,
    exclude_patterns=exclude_patterns,
    pick_latest_version=False,
):
    filelist = get_file_list(root_path, depth=depth)
    b = Builder(columns, exclude_patterns)
    df = b(filelist, cmip6_parser)
    if pick_latest_version:
        df = pick_latest_version(df)
    return df


def build_cmip5(
    root_path,
    depth=3,
    columns=cmip5_columns,
    exclude_patterns=exclude_patterns,
    pick_latest_version=False,
):
    filelist = get_file_list(root_path, depth=depth)
    b = Builder(columns, exclude_patterns)
    df = b(filelist, cmip5_parser)
    if pick_latest_version:
        df = pick_latest_version(df)
    return df


@click.command()
@click.option('--root-path', type=str)
@click.option('--depth', default=3, type=int, show_default=True)
@click.option('--pick-latest-version', default=False, is_flag=True)
@click.option('--cmip-version', type=str)
@click.option('--persist-path', type=str)
def cli(root_path, depth, pick_latest_version, cmip_version, persist_path):

    if cmip_version not in set(['5', '6']):
        raise ValueError()

    elif cmip_version == '5':
        df = build_cmip5(root_path, depth=depth, pick_latest_version=pick_latest_version)

    else:
        df = build_cmip6(root_path, depth=depth, pick_latest_version=pick_latest_version)

    if persist_path is None:
        persist_path = f'./cmip{cmip_version}.csv.gz'
    else:
        persist_path = f'{persist_path}/cmip{cmip_version}.csv.gz'

    df.to_csv(persist_path, compression='gzip', index=False)


if __name__ == '__main__':
    cli()
