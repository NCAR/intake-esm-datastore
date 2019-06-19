# Intake-esm Datastore

This repository is where intake-esm's official data collection input files live.

These input files are used by [`intake-esm`](https://github.com/NCAR/intake-esm) package when building collection catalogs.


**Adding Collection input**

To suggest adding a new collection input file, please open [an issue](https://github.com/NCAR/intake-esm-datastore/issues) or a pull request.

Whenever a new collection input file is added to this repository or an existing one is updated, **remember to update the checksum files** as well by running the `make_check_sums.py` script:

```bash
cd collection-input
python ../make_check_sums.py
```

Collections can be built using the `build_collection.py` script:

```bash
python build_collection.py collection-input/my-collection.yml --overwrite-existing
```
