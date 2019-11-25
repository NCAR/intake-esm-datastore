import fnmatch
import itertools
import re
import subprocess
from pathlib import Path

import dask
import pandas as pd
from intake.source.utils import reverse_format


class Builder:
    def __init__(self, columns, exclude_patterns=[]):
        self.columns = columns
        self.exclude_patterns = exclude_patterns

    def _filter_func(self, filelist):
        return not any(
            fnmatch.fnmatch(filelist, pat=exclude_pattern)
            for exclude_pattern in self.exclude_patterns
        )

    def _update_dict(self, entry):
        d = {key: None for key in self.columns}
        z = {**d, **entry}
        return z

    def __call__(self, filelist, parser=None):
        filelist = filter(self._filter_func, filelist)
        parsed = map(parser, filelist)
        parsed = map(self._update_dict, parsed)
        return pd.DataFrame(parsed)


def reverse_filename_format(filename, templates):
    """
    Uses intake's ``reverse_format`` utility to reverse the string method format.
    Given format_string and resolved_string, find arguments
    that would give format_string.format(arguments) == resolved_string
    """
    try:
        for template in templates:
            # Return on first successful attempt
            return reverse_format(template, filename)
    except Exception:
        print(f'Failed to parse file: {filename} using patterns: {templates}')
        return {}


def extract_attr_with_regex(input_str, regex, strip_chars=None):
    pattern = re.compile(regex, re.IGNORECASE)
    match = re.findall(pattern, input_str)
    if match:
        match = max(match, key=len)
        if strip_chars:
            match = match.strip(strip_chars)

        else:
            match = match.strip()

        return match

    else:
        return None


def get_file_list(root_path):
    from dask.diagnostics import ProgressBar

    root = Path(root_path)
    dirs = [x for x in root.iterdir() if x.is_dir()]

    @dask.delayed
    def _file_dir_files(directory):
        cmd = ['find', '-L', directory.as_posix(), '-name', '*.nc']
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        output = proc.stdout.read().decode('utf-8').split()
        return output

    filelist = [_file_dir_files(directory) for directory in dirs]
    # watch progress
    with ProgressBar():
        filelist = dask.compute(*filelist)

    filelist = list(itertools.chain(*filelist))
    return filelist