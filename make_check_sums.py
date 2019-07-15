import os
import hashlib
import glob

def _file_md5_checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, 'rb') as f:
        hash_md5.update(f.read())
    return hash_md5.hexdigest()


def main():
    files = glob.glob('*.yml')
    for yamlf in files:
        prefix = yamlf.split('.')[0]
        outf = '{}.md5'.format(prefix)

        hash_md5_new = _file_md5_checksum(yamlf)
        hash_md5_old = None
        if os.path.exists(outf):
            with open(outf, 'r') as f:
                hash_md5_old = f.read()

        if hash_md5_new != hash_md5_old:
            print(f'{yamlf} changed...updating')
            with open(outf, 'w') as f:
                f.write(hash_md5_new)
        else:
            print(f'{yamlf} no change')

if __name__ == '__main__':
    main()
