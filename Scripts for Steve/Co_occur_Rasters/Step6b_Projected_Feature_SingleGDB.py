import os

import datetime

import arcpy
# Title - Re-projects use vectors into projection by region

# in and out locations
inGDB = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroup_Union_final_20160811.gdb'
outfolder = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat'

outGDBname = 'Projected_UnionRange_20160816.gdb'
outGDB = outfolder + os.sep + outGDBname
midGBD = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\scratch.gdb'

# projection folder
prjFolder = "C:\Workspace\projections"


# List of final projections
RegionalProjection_List = ['Albers_Conical_Equal_Area.prj', 'NAD 1983 UTM Zone  4N.prj', 'NAD_1983_Albers.prj',
                           'WGS_1984_Albers.prj', 'WGS 1984 UTM Zone  2S.prj', 'WGS 1984 UTM Zone 55N.prj',
                           'NAD_1983_Albers.prj', 'WGS 1984 UTM Zone 20N.prj', 'NAD_1983_Albers.prj']

Region_Dict = {'L48': 'Albers_Conical_Equal_Area.prj',
               'HI': 'NAD_1983_Albers.prj',
               'AK': 'WGS_1984_Albers.prj',
               'AS': 'WGS 1984 UTM Zone  2S.prj',
               'CNMI': 'WGS 1984 UTM Zone 55N.prj',
               'GU': 'NAD_1983_Albers.prj',
               'PR': 'NAD_1983_Albers.prj',
               'VI': 'NAD_1983_Albers.prj',
               'Howland_Baker_Jarvis': 'NAD_1983_Albers.prj',
               'Johnston': 'NAD_1983_Albers.prj',
               'Laysan': 'NAD_1983_Albers.prj',
               'Mona': 'NAD_1983_Albers.prj',
               'Necker': 'NAD_1983_Albers.prj',
               'Nihoa': 'NAD_1983_Albers.prj',
               'NorthwesternHI': 'NAD_1983_Albers.prj',
               'Palmyra_Kingman': 'NAD_1983_Albers.prj',
               'Wake': 'NAD_1983_Albers.prj'
               }


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use vectors  and projections into final regional project
def feature_project(prj_file, prj_folder, in_gdb, region_dict):
    arcpy.env.workspace = in_gdb
    fc_list = arcpy.ListFeatureClasses()

    start_process = datetime.datetime.now()

    # location prj files
    wgs_coord_file = prj_folder + os.sep + 'WGS 1984.prj'
    prj_file_path = prj_folder + os.sep + prj_file

    # extract spatial information from prj files
    dsc_wgs = arcpy.Describe(wgs_coord_file)
    wgs_coord_sys = dsc_wgs.spatialReference

    dsc_prj = arcpy.Describe(prj_file_path)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName
    region_prj = region_dict[prj]
    prj_name = prj.replace('.prj', '')

    for fc in fc_list:
        region = fc.split('_')
        if region != region_prj:
            continue
        print "\n"
        in_fc = inGDB + os.sep + str(fc)
        print in_fc

        if prj_datum == "D_WGS_1984":
            in_fc = inGDB + os.sep + str(fc)

            fc_other_geo = str(fc) + "_WGS84"
            out_fc = midGBD + os.sep + fc_other_geo

            prj_fc_name = prj_name + "_" + fc_other_geo
            prj_fc = outGDB + os.sep + prj_fc_name

            if not arcpy.Exists(out_fc):
                arcpy.Project_management(in_fc, out_fc, wgs_coord_sys)
                print(arcpy.GetMessages(0))

            if not arcpy.Exists(prj_fc):
                arcpy.Project_management(out_fc, prj_fc, prj_sr)
                print(arcpy.GetMessages(0))
            else:
                print str(prj_fc_name) + " already exists"
        else:
            prj_fc_name = prj_name + "_" + str(fc)
            prj_fc = outGDB + os.sep + prj_fc_name

            if not arcpy.Exists(prj_fc):
                arcpy.Project_management(in_fc, prj_fc, prj_sr)
                print(arcpy.GetMessages(0))
            else:
                print str(prj_fc) + " already exists"
    print 'Completed projection of {0} in: {1}'.format(prj_name, (datetime.datetime.now() - start_process))


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_gdb(outfolder, outGDBname, outGDB)

for prj in RegionalProjection_List:
    print 'Starting Projection of raster with prj {0}'.format(prj)
    feature_project(prj, prjFolder, inGDB, Region_Dict)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
