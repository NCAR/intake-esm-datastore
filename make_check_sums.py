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
        with open(outf, 'w') as f:
            f.write(_file_md5_checksum(yamlf))

if __name__ == '__main__':
    main()