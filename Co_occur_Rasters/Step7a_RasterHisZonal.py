import os
import datetime

import arcpy
from arcpy.sa import *


# Title- runs Zonal Histogram for all sp union file against each use

# in folder with many gdbs or a single gdb
inlocation_species = 'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\ClippedProject_MaxArea.gdb'
skip_sp_gdb=['']
# set to a no zero number to skip x raster in the inlocation
start_file = 0
# raster must be set to unique values as symbology to run raster histogram
symbologyLayer = "C:\Workspace\EDM_2015\CDL_1015_100x2_euc.lyr"
# Use sites


# out_results = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\CriticalHabitat\Results'
out_results = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\Results_Clipped'
scratchpath = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\scratch.gdb'

# Dictionary of all projections needed for raster and the snap raster snap raster must be in desired
# projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {
    'CONUS': r'J:\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015',
    'HI': r'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'AK': r'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AK_Developed_euc',
    'AS': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AS_OSD_euc',
    'CNMI': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\CNMI_OSD_euc',
    'GU': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\GU_Developed_euc',
    'PR': 'J:\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\PR_OtherCrops_euc',
    'VI': 'J:\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\VI_Ag_euc',
    'Howland': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Johnston': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Laysan': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Mona': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Necker': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Nihoa': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'NorthwesternHI': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Palmyra': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Wake': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc'
}

UseGDBdict = {'CONUS': 'Conus_UseLayer.gdb',
              'HI': 'HI_UseLayer.gdb',
              'AK': 'AK_UseLayer.gdb',
              'AS': 'AS_UseLayer.gdb',
              'CNMI': 'CNMI_UseLayer.gdb',
              'GU': 'GU_UseLayer.gdb',
              'PR': 'PR_UseLayer.gdb',
              'VI': 'VI_UseLayer.gdb',
              'Howland': 'Howland_Baker_Jarvis_UseLayer.gdbj',
              'Johnston': 'Johnston_UseLayer.gdb',
              'Laysan': 'Laysan_UseLayer.gdb',
              'Mona': 'Mona_UseLayer.gdb',
              'Necker': 'Necker_UseLayer.gdb',
              'Nihoa': 'Nihoa_UseLayer.gdb',
              'NorthwesternHI': 'NortherwesternHI_UseLayer.gdb',
              'Palmyra': 'Palmyra_Kingman_UseLayer.gdb',
              'Wake': 'Wake_UseLayer.gdb'}

Region_gdb = {'Albers_Conical_Equal_Area.gdb': ['Conus'],
              'NAD_1983_UTM_Zone__4N.gdb': ['HI'],
              'WGS_1984_Albers.gdb': ['AK'],
              'WGS_1984_UTM_Zone__2S.gdb': ['AS'],
              'WGS_1984_UTM_Zone_55N.gdb': ['CNMI', 'GU'],
              'NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200.gdb': ['PR'],
              'WGS_1984_UTM_Zone_20N.gdb': ['VI'],
              'NAD_1983_Albers.gdb': ['Howland', 'Johnston', 'Laysan', 'Mona', 'Necker', 'Nihoa', 'NorthwesternHI',
                                      'Palmyra',
                                      'Wake']
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
def ZonalHist(inZoneData, inValueRaster, scratchpath, set_raster_sybbology, snap_raster_dict,region_snap_raster, processing_extent):
    start_zone = datetime.datetime.now()
    # extract spatial reference from inraster to determine the correct snap raster

    snapRaster = snap_raster_dict[region_snap_raster]
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snapRaster, "snap")
    arcpy.env.snapRaster = "snap"
    arcpy.env.extent = processing_extent

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

    dbf = outpath_final + os.sep + str(runID)
    if not arcpy.Exists(dbf):
        print dbf
        arcpy.MakeRasterLayer_management(inValueRaster, "rdlayer")
        arcpy.MakeRasterLayer_management(inZoneData, "fc_lyr")

        symbology_layer = set_raster_sybbology
        arcpy.ApplySymbologyFromLayer_management("rdlayer", symbology_layer)
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
        in_sp = inlocation_species + os.sep + raster_in
        if count < start_file:
            print in_sp
            continue
        else:
            list_regions = Region_gdb[inlocation_species]
            for region in list_regions:
                pathlist = UseGDBdict[region]
                for v in pathlist:
                    rasterLocation = v
                    for raster in rasters_in_workspace(rasterLocation):
                        inraster = rasterLocation + os.sep + raster
                        use_extent = Raster(inraster.extent)
                        ZonalHist(in_sp, inraster, scratchpath, symbologyLayer, RegionalProjection_Dict, use_extent)
else:
    workspaces = os.listdir(inlocation_species)
    gdb_workspaces = [v for v in workspaces if v.endswith('.gdb')]
    for i in gdb_workspaces:
        if i in skip_sp_gdb:
            continue
        else:
            in_gdb = inlocation_species + os.sep + i
            for raster_in in rasters_in_workspace(in_gdb):
                in_sp = in_gdb + os.sep + raster_in
                list_regions = Region_gdb[in_gdb]
                for region in list_regions:
                    pathlist = UseGDBdict[region]
                    for v in pathlist:
                        rasterLocation = v
                        for use_raster in rasters_in_workspace(rasterLocation):
                            inraster = Raster(rasterLocation + os.sep + use_raster)
                            use_extent = inraster.extent
                            ZonalHist(in_sp, inraster, scratchpath, symbologyLayer, RegionalProjection_Dict,region, use_extent)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
