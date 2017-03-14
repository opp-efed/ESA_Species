__author__ = 'JConno02'

import os
import zipfile



infolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH\WebMercator'
out = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH\WebMercator\zip_projected'
##L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH
#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH\zipped_CH
#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\ShapeWebApp_Range
#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\ShapeWebApp_Range\zipped_range
listfiles = os.listdir(infolder)
print listfiles
grouplist = []

for v in listfiles:
    if len(v.split('.'))!= 2:
        pass
    else:
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
