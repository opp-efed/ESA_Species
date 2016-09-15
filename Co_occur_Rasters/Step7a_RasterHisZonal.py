import os
import datetime
import functions

import arcpy

# Title- runs Zonal Histogram for all sp union file against each use

# in folder with many gdbs or a single gdb
inlocation_species = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\CH_SpCompRaster_byProjection\StatePlane_Puerto_Rico.gdb'
use_folder = 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject'
skip_sp_gdb = ['']
# set to a no zero number to skip x raster in the inlocation
start_file = 0
# raster must be set to unique values as symbology to run raster histogram
symbologyLayer = "C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\CDL_1015_100x2_euc.lyr"
# Use sites

region_skip = []
# out_results = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\CriticalHabitat\Results'
out_results = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\results'
scratchpath = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\results\scratch.gdb'

# Dictionary of all projections needed for raster and the snap raster snap raster must be in desired
# projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {
    'CONUS': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015',
    'HI': r'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'AK': r'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AK_Developed_euc',
    'AS': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AS_OSD_euc',
    'CNMI': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\CNMI_OSD_euc',
    'GU': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\GU_Developed_euc',
    'PR': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\PR_UseLayer.gdb\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_OtherGrains_euc',
    'VI': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\VI_Ag_euc'
}

UseGDBdict = {'CONUS': 'Conus_UseLayer.gdb',
              'HI': 'HI_UseLayer.gdb',
              'AK': 'AK_UseLayer.gdb',
              'AS': 'AS_UseLayer.gdb',
              'CNMI': 'CNMI_UseLayer.gdb',
              'GU': 'GU_UseLayer.gdb',
              'PR': 'PR_UseLayer.gdb',
              'VI': 'VI_UseLayer.gdb',
              }
# Regions outside of use extent
# 'Howland_Baker_Jarvis': 'NAD_1983_Albers.prj',
# 'Johnston': 'NAD_1983_Albers.prj',
# 'Laysan': 'NAD_1983_Albers.prj',
# 'Mona': 'NAD_1983_Albers.prj',
# 'Necker': 'NAD_1983_Albers.prj',
# 'Nihoa': 'NAD_1983_Albers.prj',
# 'NorthwesternHI': 'NAD_1983_Albers.prj',
# 'Palmyra_Kingman': 'NAD_1983_Albers.prj',
# 'Wake': 'NAD_1983_Albers.prj'
Region_gdb = {'Albers_Conical_Equal_Area.gdb': ['CONUS'],
              'NAD_1983_UTM_Zone__4N.gdb': ['HI'],
              'WGS_1984_Albers.gdb': ['AK'],
              'WGS_1984_UTM_Zone__2S.gdb': ['AS'],
              'WGS_1984_UTM_Zone_55N.gdb': ['CNMI', 'GU'],
              'StatePlane_Puerto_Rico.gdb': ['PR'],
              'WGS_1984_UTM_Zone_20N.gdb': ['VI'],

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
def ZonalHist(inZoneData, inValueRaster, scratchpath, set_raster_symbology, snap, use, region_c):
    start_zone = datetime.datetime.now()
    # In paths
    path_fc, in_species = os.path.split(inZoneData)
    sp_group = in_species.split("_")[1]
    # out paths
    break_use = use.split("_")
    break_bool = False
    use_nm = region_c

    for v in break_use:
        if v != region_c:
            pass
        else:
            break_bool = True
        if break_bool:
            if v== region_c:
                continue
            use_nm = use_nm + "_" + v
    print use_nm

    outgdb = use_nm + '.gdb'
    runID = in_species + "_" + use
    outpath_final = out_results + os.sep + outgdb
    create_gdb(out_results, outgdb, outpath_final)

    # HARD CODE TO FIELD TO BE USE AS ZONEID
    zoneField = "Value"

    dbf = outpath_final + os.sep + str(runID)
    print dbf

    if arcpy.Exists(dbf):
        print ("Already completed run for {0}".format(runID))
    else:
        try:

            functions.call_zonal_hist(inZoneData, zoneField, inValueRaster, dbf, set_raster_symbology, snap,
                                      scratchpath, outpath_final, sp_group,use_nm)
            print "Completed in {0}\n".format((datetime.datetime.now() - start_zone))
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(dbf)  # delete partial results if a run results in a error


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

count = 0
if inlocation_species[-3:] == 'gdb':
    path, gdb = os.path.split(inlocation_species)

    arcpy.env.workspace = inlocation_species
    count_sp = len(arcpy.ListRasters())
    count = 0
    for raster_in in rasters_in_workspace(inlocation_species):
        count += 1
        in_sp = inlocation_species + os.sep + raster_in
        if count < start_file:
            print in_sp
            continue
        else:
            list_regions = Region_gdb[gdb]
            for region in list_regions:
                if region in region_skip:
                    continue
                region_use_gdb = UseGDBdict[region]
                snap_raster = RegionalProjection_Dict[region]
                rasterLocation = use_folder + os.sep + region_use_gdb
                print "\nWorking on uses for {0} species file {1} of {2}".format(rasterLocation, count, count_sp)
                arcpy.env.workspace = rasterLocation
                count_use = len(arcpy.ListRasters())
                use_layer_count = 0
                for raster in rasters_in_workspace(rasterLocation):
                    inraster = rasterLocation + os.sep + raster
                    print '\nStarting use layer {0}...{1}, {2}'.format(raster, use_layer_count, count_use)
                    ZonalHist(in_sp, inraster, scratchpath, symbologyLayer, snap_raster, raster, region)
                    use_layer_count += 1
else:
    workspaces = os.listdir(inlocation_species)
    gdb_workspaces = [v for v in workspaces if v.endswith('.gdb')]
    for i in gdb_workspaces:
        if i in skip_sp_gdb:
            continue
        else:
            in_gdb = inlocation_species + os.sep + i
            arcpy.env.workspace = in_gdb
            count_sp = len(arcpy.ListRasters())
            count = 0
            for raster_in in rasters_in_workspace(in_gdb):
                in_sp = in_gdb + os.sep + raster_in
                list_regions = Region_gdb[i]
                for region in list_regions:
                    if region in region_skip:
                        continue
                    region_use_gdb = UseGDBdict[region]
                    snap_raster = RegionalProjection_Dict[region]
                    rasterLocation = use_folder + os.sep + region_use_gdb
                    print "\nWorking on uses for {0} species file {1} of {2}".format(rasterLocation, count, count_sp)
                    arcpy.env.workspace = rasterLocation
                    count_use = len(arcpy.ListRasters())
                    use_layer_count = 0
                    for raster in rasters_in_workspace(rasterLocation):
                        inraster = rasterLocation + os.sep + raster
                        print '\nStarting use layer {0}...{1}, {2}'.format(raster, use_layer_count, count_use)
                        ZonalHist(in_sp, inraster, scratchpath, symbologyLayer, snap_raster, raster)
                        use_layer_count += 1
                    count += 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
