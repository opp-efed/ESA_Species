import csv
import os
import datetime
import arcpy

# Tile: Merge files for a species that represent different sections of a range, ie nest and forageing, or buffered line
# and associated polygon then archives the individual files

#TODO Set up archive as fuvction so it can be use on other scripts

# User input variable
inGDB = 'J:\Workspace\ESA_Species\Range\NAD83\Reptiles.gdb'
archiveGDB = 'J:\Workspace\ESA_Species\Range\NAD83\ArchivedRange\Reptiles.gdb'
# final field in fc
fieldlist = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status', 'Pop_Abb']

# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


entlist = []
duplist = []

# Generates a list of duplicate values by entityID
for fc in fcs_in_workspace(inGDB):
    ent = fc.split("_")
    ent = ent[1]
    if ent in entlist:
        duplist.append(ent)
    else:
        entlist.append(ent)
print duplist

for i in duplist:
    fclist = []
    #Generates a list of fcs for the current duplicate entityid
    for fc in fcs_in_workspace(inGDB):
        ent = fc.split("_")
        ent = ent[1]
        if ent == i:
            fclist.append(fc)

    count = 0
    locationlist = []
    datelist = []

    # set local variable set equal to the fc file path for each fc and list the dates of the files
    for value in fclist:
        location = "file_" + str(count)
        vars()[location] = inGDB + os.sep + str(value)
        parse = value.split("_")
        filetype = str(parse[0])
        ent = str(parse[1])
        fctype = str(parse[2])
        date = str(parse[3])
        datelist.append(date)
        locationlist.append(vars()[location])
        count += 1

    print locationlist
    print datelist
    concat = ''
    # concatenates the dates together from the different fiels
    for value in datelist:
        new = value
        concat = str(concat) + "_" + str(new)

    print concat
    out_merge = inGDB + os.sep + filetype + "_" + ent + concat + "_temp"
    out_final = inGDB + os.sep + filetype + "_" + ent + concat + "_merged"
    print out_merge

    # executes merge
    arcpy.Merge_management(locationlist, out_merge)
    templyr = "fc_lyr"
    arcpy.Delete_management(templyr)
    arcpy.MakeFeatureLayer_management(out_merge, templyr)
    # Dissolve extra  fields in merged files
    arcpy.Dissolve_management(templyr, out_final, "EntityID")
    del templyr
    # extract spe info from the merged file
    with arcpy.da.SearchCursor(out_merge, fieldlist) as cursor:
        for row in cursor:
            ent = row[0]
            name = row[1]
            sci = row[2]
            spcode = row[3]
            vipcode = row[4]
    del cursor, row

    # adds final fields to the final file and populates them with hte species info
    try:
        arcpy.AddField_management(out_final, "FileName", "TEXT", "", "", "255", "", "NULLABLE", "NON_REQUIRED", "")
        for field in fieldlist:
            if field == fieldlist[0]:
                continue
            elif field == fieldlist[1]:
                arcpy.AddField_management(out_final, field, "TEXT", "", "", "50", "", "NULLABLE", "NON_REQUIRED", "")
            elif field == fieldlist[2]:
                arcpy.AddField_management(out_final, field, "TEXT", "", "", "50", "", "NULLABLE", "NON_REQUIRED", "")
            elif field == fieldlist[3]:
                arcpy.AddField_management(out_final, field, "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED", "")
            elif field == fieldlist[4]:
                arcpy.AddField_management(out_final, field, "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED", "")
    except:
        pass



    filename = os.path.basename(out_final)
    with arcpy.da.UpdateCursor(out_final, "FileName") as cursor:
        for row in cursor:
            row[0] = filename
            cursor.updateRow(row)
    del cursor, row

    arcpy.Delete_management(out_merge)

    # archives the two original files that were just mereged
    for fc in locationlist:
        filename = os.path.basename(fc)
        archivefc = archiveGDB + os.sep + filename
        if not arcpy.Exists(archivefc):
            arcpy.CopyFeatures_management(fc, archivefc)
        arcpy.Delete_management(fc)

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
