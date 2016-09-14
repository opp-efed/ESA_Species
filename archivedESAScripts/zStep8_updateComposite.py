import os
import time
import datetime

import arcpy

# Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = ""

Comp_file = "E:\Species\GIS\NMFS\NMFS_CompositeFiles\GDB\Final_NMFS_Composite.gdb\R_Composite_NMFS_Albers_AsOf_20140129"
Update_file = "E:\Species\GIS\NMFS\NMFS_CompositeFiles\GDB\NMFS_Composites.gdb\R_5966_poly_20140613_STD_NAD_Albers"

joinField = "EntityID"
date = 201502306

ws = "E:\Species\GIS\NMFS"
name_dir = "NMFS_CompositeFiles"

temp_dir = ws + os.sep + "tempWorkspace"

CompGDBName = "NMFS_Composites"

field = "up"
def start_times(startclock):
    start_time = datetime.datetime.fromtimestamp(startclock)
    print "Start Time: " + str(start_time)
    print start_time.ctime()


def end_times(endclock, startclock):
    start_time = datetime.datetime.fromtimestamp(startclock)
    end = datetime.datetime.fromtimestamp(endclock)
    print "End Time: " + str(end)
    print end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)


def CreateDirectory(path_dir):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)

# #############################################################

# start clock on timing script
start = time.time()
# Prints Elapse clock
start_times(start)

CreateDirectory(temp_dir)


path, filename = os.path.split(Comp_file)
file = filename[:-8]
name = file + str(date)
outfile = str(path) + os.sep + str(name)
print outfile

temp_shape = str(temp_dir) + os.sep + "TEMP_" + str(filename)+ ".shp"
arcpy.CopyFeatures_management(Comp_file, temp_shape)
print "copied temp file"

try:
    arcpy.DeleteField_management(temp_shape, [field])
except:
    print "Field needs to be added"

arcpy.AddField_management(temp_shape, field, "SHORT", "9", "", "", "", "NULLABLE", "NON_REQUIRED", "")
arcpy.MakeFeatureLayer_management(temp_shape, "fc_lyr")
arcpy.MakeFeatureLayer_management(Update_file, "up_lyr")
arcpy.AddJoin_management("fc_lyr", joinField, "up_lyr", joinField, "KEEP_COMMON")
arcpy.CalculateField_management("fc_lyr", field, "1", "PYTHON", "")

print "rows updated"
arcpy.RemoveJoin_management("fc_lyr")

WhereClause = '"' + field + '" = 1'
arcpy.SelectLayerByAttribute_management("fc_lyr", "NEW_SELECTION", WhereClause)

if int(arcpy.GetCount_management("fc_lyr").getOutput(0)) > 0:
    arcpy.DeleteRows_management("fc_lyr")
    print "rows deleted"
arcpy.DeleteField_management("fc_lyr", [field])
arcpy.Merge_management(["fc_lyr", "up_lyr"], outfile)

try:
    fields = ["CommonName", "SciName", "Name","Name_Sci"]
    uc =arcpy.da.UpdateCursor(outfile,fields)
    for row in uc:
        if (row[0] == None):
            row[0]=row [2]
        if (row[1] == None):
            row[1] = row[3]
        uc.updateRow(row)
except:
    print "No species info to update"

try:
    arcpy.DeleteField_management(outfile, "Name")
    arcpy.DeleteField_management(outfile, "Name_Sci")
except:
    print "No extra columns to delete"
##End clock time script
done = time.time()
end_times(done,start)