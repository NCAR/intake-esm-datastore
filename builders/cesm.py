import os

import yaml
from core import Builder, extract_attr_with_regex, get_asset_list
from requests.structures import CaseInsensitiveDict

with open('cesm2_cmip6.yaml') as f:
    cesm2_cmip6_definitions = yaml.safe_load(f)

component_streams = cesm2_cmip6_definitions['component_streams']
cesm2_cmip6_exps = CaseInsensitiveDict(cesm2_cmip6_definitions['experiments'])
date_str_regex = r'\d{4}\-\d{4}|\d{6}\-\d{6}|\d{8}\-\d{8}|\d{10}Z\-\d{10}Z|\d{12}Z\-\d{12}Z|\d{10}\-\d{10}|\d{12}\-\d{12}'


def cesm2_cmip6_parser(filepath):
    f1 = os.path.basename(filepath)
    date_str = extract_attr_with_regex(f1, date_str_regex)
    for component, streams in component_streams.items():
        # Loop over stream strings
        # NOTE: The order matters here!
        for stream in sorted(streams, key=lambda s: len(s), reverse=True):
            s = f1.find(stream)
            if s >= 0:
                file_stream = stream
                file_component = component
                x = f1.split(stream)
                case = x[0].strip('.')
                file_experiment = case.split('CMIP6-')[-1]
                nnn = extract_attr_with_regex(file_experiment, r'.\d{3}$')
                file_experiment = file_experiment.split(nnn)[0]
                y = x[-1].strip('.').split(date_str)
                variable = y[0].strip('.')
                break

    attrs = {}
    attrs['path'] = filepath
    attrs['case'] = case
    attrs['variable'] = variable
    attrs['date_range'] = date_str
    attrs['stream'] = file_stream
    attrs['component'] = file_component
    attrs['experiment'] = file_experiment
    try:
        case_members = cesm2_cmip6_exps[file_experiment]['case_members']
        attrs.update(case_members[case])
    except Exception:
        pass
    return attrs


def build_cesm(root_path, depth=4, exclude_patterns=['*.nc_temp_.nc']):
    columns = [
        'experiment',
        'case',
        'component',
        'stream',
        'variable',
        'date_range',
        'member_id',
        'path',
        'ctrl_branch_year',
        'ctrl_experiment',
        'ctrl_member_id',
    ]
    filelist = get_asset_list(root_path, depth=depth)
    b = Builder(columns, exclude_patterns)
    df = b(filelist, cesm2_cmip6_parser)
    return df
