import fsspec
import pandas as pd


def build_catalog(fs, bucket='ncar-cesm-lens'):
    dirs = fs.ls(bucket)
    frequencies = []
    components = ['ice_nh', 'ice_sh', 'lnd', 'ocn', 'atm']
    for d in dirs:
        if d.split('/')[-1] in components:
            f = fs.ls(d)
            frequencies.extend(f)

    stores = []
    for freq in frequencies:
        s = fs.ls(freq)
        stores.extend(s)

    entries = []

    for store in stores:
        try:
            path_components = store.split('/')
            component, frequency = path_components[1], path_components[2]
            _, experiment, variable = path_components[-1].split('.')[0].split('-')
            entry = {
                'component': component,
                'frequency': frequency,
                'experiment': experiment,
                'variable': variable,
                'path': f's3://{store}',
            }
            entries.append(entry)
        except Exception:
            print(store)
            continue

    df = pd.DataFrame(entries)
    return df


if __name__ == '__main__':
    S3_URL = 'https://stratus.ucar.edu'
    # fs = fsspec.filesystem('s3', secret=os.environ['STRATUS_SECRET_KEY'], key=os.environ['STRATUS_ACCESS_KEY'],
    #                        anon=False, client_kwargs={'endpoint_url':S3_URL})
    fs = fsspec.filesystem(
        's3', profile='stratus-cesm', anon=False, client_kwargs={'endpoint_url': S3_URL}
    )

    df = build_catalog(fs)
    df.to_csv('../catalogs/stratus-cesm1-le.csv', index=False)
