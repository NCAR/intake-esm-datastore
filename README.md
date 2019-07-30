# Intake-esm Datastore

This repository is where intake-esm's official data collection input files live.

These input files are used by [`intake-esm`](https://github.com/NCAR/intake-esm) package when building collection catalogs.


## Adding Collection Input

To suggest adding a new collection input file, please open [an issue](https://github.com/NCAR/intake-esm-datastore/issues) or a pull request.

Whenever a new collection input file is added to this repository or an existing one is updated, **remember to update the checksum files** as well by running the `make_check_sums.py` script:

```bash
cd collection-input
python ../make_check_sums.py
```

## Building Collections


Collections can be built using ``intake-esm-builder`` utility from ``intake-esm``.

### Intak-esm Installation

Intake-esm can be installed from PyPI with pip:

```console
pip install intake-esm
```

It is also available from conda-forge for conda installations:

```console
conda install -c conda-forge intake-esm
```

Once ``intake-esm`` is installed, the ``intak-esm-builder`` CLI tool is available and ready to be used:

```console
intake-esm-builder --help

Usage: intake-esm-builder [OPTIONS]

Options:
-cdef, --collection-input-definition TEXT
                                Path to a collection input YAML file or a
                                name of supported collection input(see:
                                https://github.com/NCAR/intake-esm-datastore) 
                                for list of supported collection inputs.
--overwrite-existing            Whether or not to overwrite the existing
                                database file.  [default: False]
-db, --database-dir TEXT        Directory in which to persist the built
                                collection database  [default: /glade/u/home
                                /abanihi/.intake_esm/collections]
--anon / --no-anon              Access the AWS-S3 filesystem anonymously or
                                not
--profile-name TEXT             Named profile to use when authenticating
--help                          Show this message and exit.
```

### Example

To build a collection for CMIP6 data residing on NCAR's glade, you would need to run
the following:

```console
intake-esm-builder -cdef GLADE-CMIP6 --overwrite-existing
```