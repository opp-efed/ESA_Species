import os
import datetime

import arcpy

# Title - Generate master sp comp file by species group

# NOTE NOTE if process interrupted byt user incomplete file will be generated; try except loop deletes incomplete if
# script fails due to error

# Spatial library being used for union IE CritHab or Range; will loop by species group, or use can id a specific species
# GDB
inlocation = 'L:\Workspace\ESA_Species\Range\Tool_Terrestrial\CriticalHabitat'

# CH for critical habitat or R for R


if inlocation == 'L:\Workspace\ESA_Species\Range\Tool_Terrestrial\CriticalHabitat':
    out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb'
    filetype = 'CH_'
    # species group to skip because there are no GIS files, ie there is no crithab for any lichens
    skipgroup = []
else:
    out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb'
    filetype = 'R_'
    skipgroup = []

# file suffix that will be added to fc name
file_suffix = '_Composite_MAG_20161018'

# if True will only union entid listed in ent list if false will union all entid in gdb
subset_group = False
enlistfc_name = 'SubInsects_'
# list of entids to be include in subset composite
entlist = []

# ############functions


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)

# runs merge to generate comp file
def merge_sp_files(in_ws, out_inter, subset_group, merge_list):
    if subset_group:
        out_inter = enlistfc_name + '_inter'
    if not arcpy.Exists(out_inter):
        start_merge = datetime.datetime.now()
        print "\nStarting {0} at {1}".format(out_inter, (start_merge))
        arcpy.env.workspace = in_ws
        fclist = arcpy.ListFeatureClasses()
        if subset_group:
            for fc in fclist:
                entid = fc.split('_')
                entid = str(entid[2])
                if entid in entlist:
                    merge_list.append(str(in_ws + os.sep + str(out_inter)))
        else:
            merge_list = fclist

        try:
            arcpy.Merge_management(merge_list, out_inter)
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_inter)
        print "\nCreated output {0} in {1}".format(out_inter, (datetime.datetime.now() - start_merge))
        count = int(arcpy.GetCount_management(out_inter).getOutput(0))
        if count < len(merge_list):
            print "not all files included in composite for file {0}".format(out_inter)
    else:
        print '\nAlready merged{0}'.format(out_inter)

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# list of fc that will be included in comp files

mergelist =[]
if not arcpy.Exists(out_location):
    path, gdb_file = os.path.split(out_location)
    create_gdb(path, gdb_file, out_location)

# loop through gdb from in location get a list of all species of if subset_group is true the species id in entlist then
# will generate comp file by species group or the list the user inputs

# checks to see if in location is single gdb or a folder with multiple gdb
if inlocation[-3:] == 'gdb':
    sp_group = inlocation[:-4]
    sp_group = sp_group.replace(" ", "_")
    ingdb = inlocation
    outfc_inter = out_location + os.sep + filetype + sp_group + file_suffix
    if not arcpy.Exists(outfc_inter):
        merge_sp_files(ingdb, outfc_inter, subset_group, mergelist)
else:
    list_ws = os.listdir(inlocation)
    print list_ws
    for v in list_ws:
        if v[-3:] == 'gdb':
            sp_group = v[:-4]
            sp_group = sp_group.replace(" ", "_")
            if sp_group not in skipgroup:
                ingdb = inlocation + os.sep + v

                outfc_inter = out_location + os.sep + filetype + sp_group + file_suffix
                # print outfc_inter
                if not arcpy.Exists(outfc_inter):
                    merge_sp_files(ingdb, outfc_inter, subset_group, mergelist)
            else:
                continue

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)