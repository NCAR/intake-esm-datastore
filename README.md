# Intake-esm Datastore

This repository is where intake-esm's official data collection input files lives.

These input files are used by [`intake-esm`](https://github.com/NCAR/intake-esm) package when building collection catalogs.


**Adding Collections**

To suggest adding a new collection, please open [an issue](https://github.com/NCAR/intake-esm-datastore/issues) or a pull request.

Whenever a new collection input file is added to this repository or an existing one is updated, **remember to update the checksum files** as well by running the `make_check_sums.py` script:

```bash
cd collection-definitions
python ../make_check_sums.py
```

Collections can be built using the `build_collection.py` script:

```bash
python build_collection.py collection-definitions/my-collection.yml --overwrite-existing
```
