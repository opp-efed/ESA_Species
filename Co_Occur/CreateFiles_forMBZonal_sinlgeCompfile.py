import arcpy
import os
import datetime
import functions


fieldKeep = ["FileName", "EntityID","NAME", "Name_sci", "SPCode","VIPCode","parID","SUM","ID","Shape_Length","Shape_Area","Shape_Length", "OBJECTID", "Shape","Value"]

#compFC ='J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\NL48_CH_Projected_SpGroupComposites.gdb'
compFC =r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\newComp\NL48_SpGroup_Composites_Clipped.gdb\AS_R_Snails_NL48_WGSUTMZone2S_20160111'
#
#
fishnet = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\Fishnets.gdb\AS_10000_WGSUTMZone2S'
#fishnet = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\Fishnets_NAD83.gdb\PR_10000_StatePlane_NAD83'


#outpath_working='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\Topo\R_311\Working.gdb'
#outpath_final='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\Topo\R_311\R_311.gdb'

outpath_working=r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\NonL48\AS\Snails\Working.gdb'
outpath_final=r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\NonL48\AS\Snails\AS_Snails.gdb'


def CreateDirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")

start_script = datetime.datetime.now()
print "Started: {0}".format(start_script)


resultfolder,workinggdb = os.path.split(outpath_working)
finalgdb = os.path.basename(outpath_final)
lyr= resultfolder+os.sep+'lyr'

CreateDirectory(resultfolder)
CreateDirectory(lyr)

CreateGDB(resultfolder,workinggdb,(resultfolder+os.sep+workinggdb))
CreateGDB(resultfolder,finalgdb,(resultfolder+os.sep+finalgdb))

arcpy.Delete_management("comp")
arcpy.Delete_management("fish")
arcpy.Delete_management("SJ")
arcpy.MakeFeatureLayer_management(compFC,"comp")
arcpy.MakeFeatureLayer_management(fishnet,"fish")

arcpy.AddField_management("comp",'parID',"LONG")
arcpy.AddField_management("comp",'SUM',"DOUBLE")

rows = arcpy.da.UpdateCursor(compFC, ("OBJECTID","parID"))
for row in rows:
    row[1]= row[0]
    rows.updateRow(row)

filename = os.path.basename(compFC)
outFCSJ= outpath_working +os.sep +filename +'_SJ'
outFCMB = outpath_working +os.sep + filename  +'_MB'


if not arcpy.Exists(outFCSJ):
    arcpy.SpatialJoin_analysis("comp", fishnet, outFCSJ, "JOIN_ONE_TO_ONE", "KEEP_ALL","#", "HAVE_THEIR_CENTER_IN", "", "")
if not arcpy.Exists(outFCMB):
    arcpy.MinimumBoundingGeometry_management(outFCSJ, outFCMB,"RECTANGLE_BY_AREA", "LIST", "ID")

SJ_listfields = [f.name for f in arcpy.ListFields(outFCSJ)]
MB_listfields = [f.name for f in arcpy.ListFields(outFCMB)]

delfield_SJ = []
delfield_MB =[]
for f in SJ_listfields:
    vslue =str(f)
    if vslue not in fieldKeep:
        delfield_SJ.append(f)


for f in MB_listfields:
    vslue =str(f)
    if vslue not in fieldKeep:
        delfield_MB.append(f)

print delfield_SJ
print delfield_MB

if len(delfield_SJ) > 0:
    arcpy.DeleteField_management(outFCSJ, delfield_SJ)
if len(delfield_MB) > 0:
    arcpy.DeleteField_management(outFCMB, delfield_MB)


outFC_SJ = outpath_final + os.sep +filename  +'_SJ'
outFC_MB =  outpath_final +os.sep + filename  +'_MB'

arcpy.CopyFeatures_management(outFCSJ,outFC_SJ)
arcpy.CopyFeatures_management(outFCMB,outFC_MB)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)