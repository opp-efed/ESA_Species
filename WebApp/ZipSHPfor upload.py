__author__ = 'JConno02'

import os
import zipfile

infolder = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\ShapesforUpload\Range'
out = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\ShapesforUpload\zip\zipR'
#
#
listfiles = os.listdir(infolder)
print listfiles
grouplist = []

for v in listfiles:
    group = v.split('_')
    #print group
    ftype = group[0]
    group = group[1]
    if group not in grouplist:
        grouplist.append(group)

print grouplist
for v in grouplist:
    outzip = ftype + '_' + str(v)
    zfpath = os.path.join(out, (outzip + '.zip'))
    print zfpath
    zf = zipfile.ZipFile(zfpath, "w", zipfile.ZIP_DEFLATED)
    rootlen = len(infolder) + 1 ## can use this to extract all charatcter of a path structure until you reach the place to include ie replace files in the last line to filelocation[rootlen:]
    for files in listfiles:
        if files.startswith(outzip):
            print files
            filelocation = os.path.join(infolder, files)
            zf.write(filelocation, files)
            ##zipname.write (path to file, how much of the path tree to include blank this
            # will zip all files in path structure includein the files at the end)
    zf.close()
