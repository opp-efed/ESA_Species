import csv
import os
import datetime
import arcpy
import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Tile: Merge files for a species that represent different sections of a range, ie nest and foraging, or buffered line
# and associated polygon then archives the individual files

# TODO Set up archive as fuvction so it can be use on other scripts
# Merged files discussed and agreed upon with the ESA team fall 2015; revisited and stayed the same fall 2017

# User input variable
masterlist = r"\MasterListESA_Feb2017_20190130.csv"
group_colindex = 16 # Index position of the group column in the master species list

# in and out workspaces - spatial libraries generalize or non for \range or \criticalhabitat
infolder = r'path\CriticalHabitat'
archivefolder = 'path\CriticalHabitat\Archived'

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

masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df.ix[:,group_colindex]
sp_groups_df = sp_groups_df.drop_duplicates()
alpha_group = sorted(sp_groups_df.values.tolist())


for group in alpha_group:
    entlist = []
    duplist = []

    print "\nWorking on {0}".format(group)
    inGDB =  infolder + os.sep + str(group) + '.gdb'
    archiveGDB = archivefolder + os.sep + str(group) + '.gdb'


    arcpy.env.workspace = inGDB
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
        datelist = list(set(datelist))
        print locationlist
        print datelist
        concat = ''
        # concatenates the dates together from the different fiels
        for value in datelist:
            new = value
            concat = str(concat) + "_" + str(new)

        print concat
        out_merge = inGDB + os.sep + filetype + "_" + ent + concat + "_temp"
        out_final = inGDB + os.sep + filetype + "_" + ent +  "_merged" + concat +'_STD_NAD83'
        print out_merge

        # executes merge
        if not arcpy.Exists(out_merge):
            arcpy.Merge_management(locationlist, out_merge)
        templyr = "fc_lyr"
        arcpy.Delete_management(templyr)
        arcpy.MakeFeatureLayer_management(out_merge, templyr)
        # Dissolve extra  fields in merged files
        if not arcpy.Exists(out_final):
            arcpy.Dissolve_management(templyr, out_final, "EntityID")
        del templyr
        # # extract spe info from the merged file
        # with arcpy.da.SearchCursor(out_merge, fieldlist) as cursor:
        #     for row in cursor:
        #         ent = row[0]
        #         name = row[1]
        #         sci = row[2]
        #         spcode = row[3]
        #         vipcode = row[4]
        # del cursor, row

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
