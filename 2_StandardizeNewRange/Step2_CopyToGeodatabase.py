import os
import csv
import datetime
import pandas as pd
import arcpy
# This script will copy feature classes to the designated gdb, the orginal names will be maintained
# All subfolders/workspaces within the location assigned as the workspace with the env.workspace variable
# TODO: Can this be streamlined to match the updated format of the FWS files (composites) without impacting NMFS files
# Tile: Copy shapefiles to a geodatabase and generate list of files.


# Variables to be set by user

# True updates came in as a composite file, ie multiple species in one file and not individual files
# False if individual files by species
compfile = True
# column in the composite file used to identify a species typically the entityID, use '' if individual files
entid_col_comp = "ENTITY_ID"
NmCSVCopied = 'FWS_Range_Updated2019'  # name for output table
out_nameGDB = "FWS_Updated2019"  # name for staging gdb

# location of new species files
# ***this must be a folder not a .shp or .gdb
InFileLocations = "L:\Workspace\StreamLine\Species Spatial Library\Download_FWS_Jan2019\FilteredSection7\Range"
# master species list
masterlist = r"L:\Workspace\StreamLine\Species Spatial Library\MasterListESA_Feb2017_NeedCH_20181203.csv"
entityid_col_master = 'EntityID'
# Workspace
# path to output workspace
# Workspace
ws = "L:\Workspace\StreamLine\Species Spatial Library\UpdateFiles"
# Folder in workspace where outputs will be saved
name_dir = "UpdatedProcess_Jan2019"

# in yyyymmdd received date
receivedDate = '20190130'


# FUNCTIONS


def fcs_in_workspace(workspace):
    # recursively checks workspaces found within the inFileLocation and makes list of all feature class
    arcpy.env.workspace = workspace
    for fcs in arcpy.ListFeatureClasses():
        yield (fcs)
    for wks in arcpy.ListWorkspaces():
        for fcs in fcs_in_workspace(wks):
            yield (fcs)


def create_directory(path_directory, out_csv, out_gdb):
    # creates directories to save files
    if not os.path.exists(path_directory):
        os.mkdir(path_directory)
        print "created directory {0}".format(path_directory)
    if not os.path.exists(out_csv):
        os.mkdir(out_csv)
        print "created directory {0}".format(out_csv)
    if not os.path.exists(out_gdb):
        os.mkdir(out_gdb)
        print "created directory {0}".format(out_gdb)


def create_flnm_timestamp(namefile, outlocation, date_list, file_extension):
    # creates date stamped generic file
    file_extension.replace('.', '')
    filename = str(namefile) + "_" + str(date_list[0]) + '.' + file_extension
    filepath = os.path.join(outlocation, filename)
    return filename, filepath


def create_out_table(list_name, csv_name):
    # outputs table from list generated in create FileList
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


def create_gdb(out_folder, out_name, outpath):
    # Create a new GDB
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def copy_fc_to_geo(in_location, out_location_path, file_list, failed_list, entid_col, list_species):
    # if part of function executes if species files are received as individual files for species
    # else part of function added when FWS started to load files to ECOS as composites - this has not been tested
    # copies updated files to staging geodatabase
    if not compfile:
        for fc in fcs_in_workspace(in_location):
            try:
                basename, extension = os.path.splitext(fc)
                out_fc = arcpy.ValidateTableName(basename)  # replaces any invalid characters to underscores
                out_feature_class = out_location_path + os.sep + str(out_fc)
                if not arcpy.Exists(out_feature_class):
                    add_new = str(fc) + "," + str(out_fc)
                    print add_new
                    file_list.append(add_new)
                    arcpy.CopyFeatures_management(fc, out_feature_class)
                else:
                    print "FC already copied"
            except Exception as error:
                print(error.args[0])
                print "Failed  " + str(fc)
                add_failed = str(fc)
                failed_list.append(add_failed)
                if arcpy.Exists(out_feature_class):
                    # Deletes files that was interrupted during copy it would be incomplete
                    arcpy.Delete_management(out_feature_class)
        return file_list, failed_list
    else:
        for fc in fcs_in_workspace(in_location):
            basename, extension = os.path.splitext(fc)
            out_fc_prefix = arcpy.ValidateTableName(basename)  # replaces any invalid characters to underscores

            arcpy.Delete_management ('update_lyr')
            arcpy.MakeFeatureLayer_management(in_location +os.sep+fc,'update_lyr')
            list_fields = [f.name for f in arcpy.ListFields('update_lyr')]
            if 'Shape' in list_fields:  # Shape is 2-dimensions; 2-d data needs to be removed
                list_fields.remove('Shape')
            att_array = arcpy.da.TableToNumPyArray('update_lyr',list_fields)
            att_df = pd.DataFrame(data=att_array)
            del att_array  # deletes temp array
            del list_fields  # deletes field list
            # remove decimal if str loaded as number and set field equal to str
            att_df[entid_col] = att_df[entid_col].map(lambda x: str(x).split('.')[0]).astype(str)
            for entid in list(set(att_df[entid_col].values.tolist())):
                if entid not in list_species:
                    continue
                else:
                    whereclause =""""{0}" = {1}""".format(entid_col, entid)  # if the Entity_ID field is a number
                    #whereclause =""""{0}" = {2}{1}{3}""".format(entid_col, entid,"'","'")  # if the Entity_ID field is a str

                    try:
                        out_feature_class = out_location_path + os.sep + str(out_fc_prefix) + "_" + entid
                        if not arcpy.Exists(out_feature_class):
                            # Makes a feature layer that will only include current entid using whereclause
                            arcpy.Delete_management("lyr")
                            arcpy.MakeFeatureLayer_management(fc, "lyr", whereclause)
                            arcpy.CopyFeatures_management("lyr", out_feature_class)  # copies file for individual species
                        else:
                            print "FC already copied"
                    except Exception as error:
                        print(error.args[0])
                        print "Failed  " + str(fc)
                        print "Check ther data type of EntityID call and adjust where clause as needed"
                        add_failed = str(fc)
                        failed_list.append(add_failed)
                        if arcpy.Exists(out_feature_class):
                            # Deletes files that was interrupted during copy it would be incomplete
                            arcpy.Delete_management(out_feature_class)
        return file_list, failed_list


# static variable no user input needed unless making change to script structure
datelist = []
today = datetime.date.today()
datelist.append(today)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"
NmCSVFailed = 'Failed_' + str(NmCSVCopied)

# Creates file names for out tables with timestamp
OutGDB, outFilegdbpath = create_flnm_timestamp(out_nameGDB, OutFolderGDB, datelist, 'gdb')
csvfile, csvpath = create_flnm_timestamp(NmCSVCopied,
                                         outLocationCSV, datelist,
                                         'csv')  # CSV out table succeed file-complete spatial package
failedcsv, failedcsvpath = create_flnm_timestamp(NmCSVFailed,
                                                 outLocationCSV, datelist,
                                                 'csv')  # CSV out table succeed file-incomplete spatial package


# Empty list used to store information for tables
OrgSRList = []
FailedList = []
# Heading for tables
addSRList = "Original filename" + "," + "Filename in GDB: " + str(OutGDB)
OrgSRList.append(addSRList)
addFailed = "Filename-Original"
FailedList.append(addFailed)

arcpy.env.scratchWorkspace = ""
# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.workspace = OutFolderGDB


# Start  clock time script
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

masterlist_df = pd.read_csv(masterlist)
all_current_species = masterlist_df[entityid_col_master].values.tolist()
create_directory(path_dir, outLocationCSV, OutFolderGDB)

# Creates outFile GDB and sets location to InFiles allows to overwritten
create_gdb(OutFolderGDB, OutGDB, outFilegdbpath)


# Copy all spatial files found to a file geodatabase created above
addSRList, FailedList = copy_fc_to_geo(InFileLocations, outFilegdbpath, OrgSRList, FailedList, entid_col_comp,all_current_species)

# write data store in lists to out tables in csv format
create_out_table(OrgSRList, csvpath)
create_out_table(FailedList, failedcsvpath)

# End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
