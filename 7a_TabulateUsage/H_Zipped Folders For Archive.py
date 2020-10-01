__author__ = 'JConno02'

import os
import zipfile



infolder = 'E:\Workspace\StreamLine\ESA\Results_HabEle\NL48\Range\Agg_Layers'
out = 'E:\Workspace\StreamLine\ESA\Results_HabEle\NL48\Range\Agg_Layers'

listfiles = os.listdir(infolder)
listfiles = [t for t in listfiles if not t.endswith('.zip')]
print listfiles

for v in listfiles:

    outzip =  str(v)
    zfpath = os.path.join(out, (outzip + '.zip'))
    if not os.path.exists(zfpath):
        print zfpath
        zf = zipfile.ZipFile(zfpath, "w", zipfile.ZIP_DEFLATED, allowZip64=True)
        csv_location = infolder +os.sep + v
        list_csv = os.listdir(infolder +os.sep + v)

        for files in list_csv:
            if files.endswith('.csv'):
                print files
                filelocation = os.path.join(csv_location, files)
                zf.write(filelocation, files)

        zf.close()