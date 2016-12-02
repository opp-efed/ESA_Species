__author__ = 'JConno02'

import os
import zipfile

infolder = r'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Insects_GAP'
out = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Sent_FWS'
outzip = 'Lepidoptera_GAP_20160613'
entlist = ['5067', '438', '444', '446', '430', '462', '3412', '419', '450', '8083', '1984', '420', '433', '451', '421',
           '422', '4308', '5168', '4508', '423', '424', '9001', '425', '431', '432', '434', '10147', '10007', '426',
           '3670', '455', '427', '429', '428', '7495', '437']


listfiles = os.listdir(infolder)
print listfiles
grouplist = []

zfpath = os.path.join(out, (outzip + '.zip'))
print zfpath
zf = zipfile.ZipFile(zfpath, "w", zipfile.ZIP_DEFLATED)
added_zip =[]

for v in listfiles:
    ent = v.split('_')
    #print group
    entid = (str(ent[4])).replace('.csv','')
    print entid
    rootlen = len(infolder) + 1 ## can use this to extract all charatcter of a path structure until you reach the place to include ie replace files in the last line to filelocation[rootlen:]
    if entid in entlist:
        added_zip.append(entid)
        filelocation = os.path.join(infolder, v)
        zf.write(filelocation, v)
zf.close()

print added_zip

