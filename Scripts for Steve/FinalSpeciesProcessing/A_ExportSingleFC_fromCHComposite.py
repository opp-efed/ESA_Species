import arcpy
import os
import datetime

# NOTE NOTE download from ECOS does not have species identifers; need to add the concat field and EntityID fields
# before starting

# Goal
# export downloaded CH file form ECOS into indvidual species files loops through the composite file form each, selects
# selects each row base on the entityID value, makes a feature layer then exports FC to out location

# User variables
inCH_Comp_fromECOS = r'J:\Workspace\ESA_Species\CriticalHabitat\RawDownload\Downloaded_20160819_updated20160728' \
                     r'\CritHabPoly_UpdatedforFinalBE_20160809.shp'

# Must set entityid to str and have col header in the first position in list, other two used for filename
Fields = ['EntID_str', 'Concat', 'sciname']

out_location = r'J:\Workspace\ESA_Species\CriticalHabitat\RawDownload\Downloaded_20160819_updated20160728' \
               r'\CritHab_Poly_UpdatedFinalBE_20160809.gdb'

start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

# Start loop
fc = inCH_Comp_fromECOS
arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
count = arcpy.GetCount_management("fc_lyr")
count = int(count.getOutput(0))
counter = count

for row in arcpy.da.SearchCursor("fc_lyr", Fields):
    entid = str(row[0])
    Concat = row[1]
    sciname = row[2]
    sciname = sciname.replace(" ", "_")
    sciname = sciname.replace(".", "")

    outFeatureClass = out_location + os.sep + str(Concat) + "_" + str(entid) + "_" + str(sciname)
    if not arcpy.Exists(outFeatureClass):
        print '{0} files remaining of {1}, file {2} saved'.format(counter, count, outFeatureClass)
        whereclause = "EntID_str= '%s'" % entid
        arcpy.MakeFeatureLayer_management(fc, "lyr", whereclause)
        arcpy.CopyFeatures_management("lyr", outFeatureClass)
        arcpy.Delete_management("lyr")
        counter -= 1
    else:
        counter -= 1
        continue

arcpy.Delete_management("fc_lyr")

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
