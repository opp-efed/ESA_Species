import arcpy
import os
import datetime
import pandas as pd

# #TODO archive line feature class to line GDB
# Tile: Will buffer point and line features by 15 meters, this buffer distance can be adjust with the buffer_distance
# variable

# #################User input variable
# in and out workspaces
infolder = r'C:\Users\JConno02\One_Drive_fail\Documents_C_drive\Projects\ESA\_ExternalDrive' \
           r'\_CurrentSpeciesSpatialFiles\SpatialLibrary\Generalized files\CriticalHabitat'
archivefolder = 'C:\Users\JConno02\One_Drive_fail\Documents_C_drive\Projects\ESA\_ExternalDrive' \
                '\_CurrentSpeciesSpatialFiles\SpatialLibrary\Generalized files\CriticalHabitat\Archived'

masterlist = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
             '\_ExternalDrive\_CurrentSupportingTables\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
buffer_distance = "15 METERS"  # Buffer distance set by ESA team fall 2016
date = 20171204
group_colindex = 16


# recursively checks workspaces found within the inFileLocation and makes list of all feature class


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fcs in arcpy.ListFeatureClasses():
        yield (fcs)
    for wks in arcpy.ListWorkspaces():
        for fcs in fcs_in_workspace(wks):
            yield (fcs)


path_folder, tail = os.path.split(infolder)

if tail == 'Range':
    file_type = 'R_'
else:
    file_type = 'CH_'


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def archive_files(in_fc, fc_nm, archive_gdb, archived_date):
    try:
        out_fc_archive = archive_gdb + os.sep + fc_nm + "_buffered_" + archived_date
        if not arcpy.Exists(out_fc_archive):
            print "Archived: {0} because it was buffered".format(in_fc)
            arcpy.CopyFeatures_management(in_fc, out_fc_archive)
            arcpy.Delete_management(in_fc)

    except KeyError:
        pass


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

fclist = arcpy.ListFeatureClasses()
masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df.ix[:, group_colindex]
sp_groups_df = sp_groups_df.drop_duplicates()
alpha_group = sorted(sp_groups_df.values.tolist())

duplicate_files = []
for group in alpha_group:

    print "\nWorking on {0}".format(group)

    # Set workspace for in location and archive location and get a list of fcs in the inlocation
    ingdb = infolder + os.sep + str(group) + '.gdb'
    archivegdb = archivefolder + str(group) + '.gdb'
    arcpy.env.workspace = ingdb
    fclist = arcpy.ListFeatureClasses()

    # loops on fcs and if file is a point or line executes the buffer to the distance set by user
    for fc in fclist:
        infile = ingdb + os.sep + fc
        fcname = fc.split("_")
        line = str(fcname[2])
        if line == 'line':
            ent = str(fcname[1])
            outfile = ingdb + os.sep + file_type + ent + "_lineBuffer_" + str(date) + "_STD_NAD83"
            if not arcpy.Exists(outfile):
                arcpy.Buffer_analysis(infile, outfile, buffer_distance, "FULL", "ROUND", "LIST")
                try:
                    arcpy.AddField_management(outfile, "EntityID", "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED",
                                              "")
                except Exception as error:
                    print(error.args[0])
                    pass
                # updates the entity id field to the entity id extracted from the file name
                rows = arcpy.UpdateCursor(outfile)
                for row in rows:
                    row.setValue("EntityID", ent)
                    rows.updateRow(row)
                del rows
                print "Print buffer {0}".format(fc)
                archive_files(infile, fc, archivegdb, str(date))
        elif line == 'point':
            ent = str(fcname[1])
            outfile = ingdb + os.sep + file_type + ent + "_lineBuffer_" + str(date) + "_STD_NAD83"
            if not arcpy.Exists(outfile):
                arcpy.Buffer_analysis(infile, outfile, buffer_distance, "FULL", "ROUND", "LIST")
                try:
                    arcpy.AddField_management(outfile, "EntityID", "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED",
                                              "")
                except Exception as error:
                    print(error.args[0])
                    pass
                # updates the entity id field to the entity id extracted from the file name
                rows = arcpy.UpdateCursor(outfile)
                for row in rows:
                    row.setValue("EntityID", ent)
                    rows.updateRow(row)
                del rows
                print "Print buffer {0}".format(fc)
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
