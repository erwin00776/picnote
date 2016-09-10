import os
import sys
import gzip
import zipfile


def ungz(file_path):
    file_name = os.path.basename(file_path)
    file_name = file_name.replace(".gz", '')
    g_file = gzip.GzipFile(file_path)
    dst = os.path.join('/tmp/uncompress', file_name)
    open(dst, 'w+').write(g_file.read())
    g_file.close()
    return dst


def unzip(file_path):
    if not os.path.exists(file_path):
        return None
    file_name = os.path.basename(file_path)
    zip_file = zipfile.ZipFile(file_path)
    dst = os.path.join('/tmp/uncompress', file_name)
    if os.path.exists(dst):
        pass
    else:
        os.makedirs(dst)
    for names in zip_file.namelist():
        zip_file.extract(names, dst)
    zip_file.close()
    return dst



if __name__ == '__main__':
    tmp = unzip('/home/erwin/Documents/test.odt')
    # lines = odt_file_reader(os.path.join(tmp, 'content.xml'))
    # lines = odt_file_reader('/home/erwin/Documents/test.odt')
    # print(lines)

    # ungz('/home/erwin/Documents/test.odt.gz')