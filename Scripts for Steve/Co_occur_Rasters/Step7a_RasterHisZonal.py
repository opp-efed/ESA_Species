import os
import datetime

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

# in folder with many gdbs or a single gdb
inlocation_species = 'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\ClippedProject_MaxArea.gdb'
# set to a no zero number to skip x raster in the inlocation
start_file = 0
# raster must be set to unique values as symbology to run raster histogram
symbologyLayer = "C:\Workspace\EDM_2015\CDL_1015_100x2_euc.lyr"
# Use sites
pathlist = [r'C:\Workspace\EDM_2015\CDL1015x_mosaic2_euc.gdb']

# out_results = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\CriticalHabitat\Results'
out_results = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\Results_Clipped'
scratchpath = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\scratch.gdb'

# Dictionary of all projections needed for raster and the snap raster snap raster must be in desired
# projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {'Albers_Conical_Equal_Area.prj': r'J:\Cultivated_Layer\2015_Cultivated_Layer'
                                                            r'\2015_Cultivated_Layer.gdb\cultmask_2015',
                           # 'NAD 1983 UTM Zone  4N.prj': ,'',
                           # 'NAD_1983_Albers.prj':'',
                           # WGS_1984_Albers.prj':'',
                           # 'WGS 1984 UTM Zone  2S.prj':'',
                           # 'WGS 1984 UTM Zone 55N.prj' :'',
                           # 'NAD_1983_Albers.prj' :'',
                           # 'WGS 1984 UTM Zone 20N.prj': '',
                           # 'NAD_1983_Albers.prj':''
                           }


# recursively looks for all raster in workspace
def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loops runs zonal histogram for union files
def ZonalHist(inZoneData, inValueRaster, scratchpath, set_raster_sybbology, snap_raster_dict):
    start_zone = datetime.datetime.now()
    # extract spatial reference from inraster to determine the correct snap raster
    ORGdsc = arcpy.Describe(inValueRaster)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name()
    snapRaster = snap_raster_dict[ORGprj]
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snapRaster, "snap")
    arcpy.env.snapRaster = "snap"

    # In paths
    path, use_raster = os.path.split(inValueRaster)
    path_fc, in_species = os.path.split(inZoneData)
    # out paths
    outgdb = use_raster + '.gdb'
    runID = use_raster + "_" + in_species
    outpath_final = out_results + os.sep + outgdb
    create_gdb(out_results, outgdb, outpath_final)

    # HARD CODE TO FIELD TO BE USE AS ZONEID
    zoneField = "Value"
    arcpy.env.scratchWorkspace = scratchpath
    arcpy.env.workspace = outpath_final
    arcpy.CheckOutExtension("Spatial")


    # arcpy.env.overwriteOutput = True
    # Results table
    dbf = outpath_final + os.sep + str(runID)
    if not arcpy.Exists(dbf):
        print dbf
        arcpy.MakeRasterLayer_management(inValueRaster, "rdlayer")
        arcpy.MakeRasterLayer_management(inZoneData, "fc_lyr")

        symbologyLayer = set_raster_sybbology
        arcpy.ApplySymbologyFromLayer_management("rdlayer", symbologyLayer)
        try:
            print ("Running Statistics...for species group {0} and raster {1}".format(in_species, use_raster))
            ZonalHistogram("fc_lyr", zoneField, "rdlayer", dbf)
            print "Completed in {0}\n".format((datetime.datetime.now() - start_zone))
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(dbf)  # delete partial results if a run results in a error
        arcpy.Delete_management("rdlayer")
        arcpy.Delete_management("fc_lyr")
    else:
        print ("Already completed run for {0}".format(runID))


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

count = 0
if inlocation_species[-3:] == 'gdb':
    for raster_in in rasters_in_workspace(inlocation_species):
        print count
        count += 1
        infc = inlocation_species + os.sep + raster_in
        if count < start_file:
            print infc

            continue
        else:

            for v in pathlist:
                rasterLocation = v
                for raster in rasters_in_workspace(rasterLocation):
                    inraster = rasterLocation + os.sep + raster
                    ZonalHist(infc, inraster, scratchpath, symbologyLayer, RegionalProjection_Dict)
else:
    infc = inlocation_species
    for v in pathlist:
        rasterLocation = v
        for raster in rasters_in_workspace(rasterLocation):
            inraster = rasterLocation + os.sep + raster
            ZonalHist(infc, inraster, scratchpath, symbologyLayer, RegionalProjection_Dict)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)