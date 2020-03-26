# Scripts for building catalogs

- [Scripts for building catalogs](#scripts-for-building-catalogs)
  - [Create conda environment](#create-conda-environment)
  - [CMIP5 and CMIP6](#cmip5-and-cmip6)

## Create conda environment

```bash
conda env update -f environment.yaml
```

Once the environment creation step is done, activate the created environment:

```bash
conda activate esm-catalog-builder
```

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
python cmip.py --root-path /glade/collections/cmip/CMIP6 --pick-latest-version --cmip-version 6 --csv-filepath ../catalogs/glade-cmip6.csv.gz --depth 5
```
