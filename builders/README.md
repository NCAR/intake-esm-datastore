# Scripts to build catalogs

- [Scripts to build catalogs](#scripts-to-build-catalogs)
  - [CMIP5 and CMIP6](#cmip5-and-cmip6)
  - [TODO: CESM](#todo-cesm)

## CMIP5 and CMIP6

```bash
$ python cmip.py --help
Usage: cmip.py [OPTIONS]

Options:
  --root-path PATH            Root path of the CMIP project output.
  -d, --depth INTEGER         Recursion depth. Recursively walk root_path to a
                              specified depth  [default: 4]
  --pick-latest-version       Whether to only catalog lastest version of data
                              assets or keep all versions  [default: False]
  -v, --cmip-version INTEGER  CMIP phase (e.g. 5 for CMIP5 or 6 for CMIP6)
  --csv-filepath TEXT         File path to use when saving the built catalog
  --help                      Show this message and exit.
```

**Example:**

```bash
$ python cmip.py --root-path /glade/collections/cmip/CMIP6 --pick-latest-version --cmip-version 6 --csv-filepath ../catalogs/glade-cmip6.csv.gz --depth 5
```

## TODO: CESM
