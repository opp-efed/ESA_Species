import arcpy
import os
import datetime
import csv


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield (fc)
# recursively checks workspaces found within the inFileLocation and makes list of all rasters
def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster

# creates directories to save files
def create_directory(path_dir, outLocationCSV, OutFolderGDB):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)
    if not os.path.exists(outLocationCSV):
        os.mkdir(outLocationCSV)
        print "created directory {0}".format(outLocationCSV)
    if not os.path.exists(OutFolderGDB):
        os.mkdir(OutFolderGDB)
        print "created directory {0}".format(OutFolderGDB)

# creates date stamped generic file
def create_flnm_timestamp(namefile, outlocation, date_list, file_extension):
    file_extension.replace('.', '')
    filename = str(namefile) + "_" + str(date_list[0]) + '.' + file_extension
    filepath = os.path.join(outlocation, filename)
    return filename, filepath

# outputs table from list generated in create FileList
def create_out_table(list_name, csv_name):
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])
# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
# creates date stamped csv
def create_csvflnm_timestamp(namecsvfile, outcsvlocation, date_list):
    filename = str(namecsvfile) + "_" + str(date_list[0]) + '.csv'
    filepath = os.path.join(outcsvlocation, filename)
    return filename, filepath

# export dictionaries or lists to csv final tables
def create_outtable(outInfo, csvname, header):

    if type(outInfo) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for k, v in outInfo.items():
                val = []
                val.append(k)
                val.append(outInfo[k])
                writer.writerow(val)
    elif type(outInfo) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for val in outInfo:
                writer.writerow([val])


# recursively check workspaces found within the inFileLocation and makes list of all feature class the return value
# include the full path to the feature class
def fcswpath_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield os.path.join(workspace, fc)
        for ws in arcpy.ListWorkspaces():
            for fc in fcs_in_workspace(os.path.join(workspace, ws)):
                yield fc


# Start timer in day of week , month, day hour minute second year
def start_times():
    start_time = datetime.datetime.now()
    print "Start Time: " + str(start_time)
    print start_time.ctime()
    return start_time


# End timer in day of week , month, day hour minute second year
def end_times(start_time):
    end = datetime.datetime.now()
    print "End Time: " + end.ctime()

    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)
# Create dictionary based on input csv table
def createdicts(csvfile):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname