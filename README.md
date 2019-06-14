# Intake-esm Datastore

This repository is where intake-esm's official data collection input files lives.

These input files are used by [`intake-esm`](https://github.com/NCAR/intake-esm) package when building collection catalogs.

Whenever a new collection input file is added to this repository or an exisiting one is update, remember to update checksum files as well by running the `make_check_sums.py` script:

```bash
cd collection-definitions
python ../make_check_sums.py
```
