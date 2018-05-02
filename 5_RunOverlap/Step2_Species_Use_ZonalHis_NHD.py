import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- Runs overlap using Zonal Histogram for all RASTER to RASTER analyses including:
#           1) NHD catchment rasters to aggregated layers, AA, Ag and NonAG using featureID as the values

# Static variables are updated once per update; user input variables update each  run

# #### User input variables

# Update once then remains static to set file structure
use_location_base = 'L:\Workspace\UseSites\ByProjection'
out_results = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ED_results\Results'

in_location_nhd_base = r'L:\ESA\UnionFiles_Winter2018\NHDFiles\L48\NHD_ByProjection\NHD_Grids_byProjection'
in_location_nhd_folder = 'CONUS_Albers_Conical_Equal_Area'
# CONUS_Albers_Conical_Equal_Area
temp_file = "temp_table100"
run_group = 'UseLayers'
use_list = []

# ################Static variables
in_location_nhd = in_location_nhd_base + os.sep + in_location_nhd_folder
region = os.path.basename(in_location_nhd).split("_")[0]  # folder with composite must start with region abb
use_location = use_location_base + os.sep + str(region) + "_" + run_group + ".gdb"
arcpy.env.workspace = use_location
if len(use_list) == 0:
    use_list = (arcpy.ListRasters())  # run all layers in use location
print use_list
count_use = len(use_list)
split_by = 2
find_file_type = in_location_nhd.split(os.sep)

if os.path.basename(use_location).startswith('CONUS'):
    out_results = out_results + os.sep + 'L48' + os.sep + 'NHD'
else:
    out_results = out_results + os.sep + 'NL48' + os.sep + 'NHD'

# Each region is a list pos 0 euc distance, pos 1 on/off, pos 2 Yearly (CONUS only)
symbology_dict = {'CONUS': [
    'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_1016_110x2_euc.lyr',
    r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_OnOff_X7072_171227.lyr',
    r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_2010_rec.lyr'],
    'HI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_HI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_CCAP_HI_6.lyr'],
    'AK': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_NLCD_2011_81.lyr'],
    'AS': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_AS_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_CCAP_AS_6.lyr'],
    'CNMI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
             r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_CNMI_6.lyr'],
    'GU': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_GU_6_30.lyr'],
    'PR': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_NLCD_81.lyr'],
    'VI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_CCAP_VI_6_30.lyr']}

snap_raster_dict = {'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                             r'\Albers_Conical_Equal_Area_cultmask_2016',
                    'HI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
                    'AK': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
                    'AS': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
                    'CNMI': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
                    'GU': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
                    'PR': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
                    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}


current_use = 1


# ################Functions

def zone(zone_lyr, raster_lyr, temp_table, snap):
    # Set Snap Raster environment
    arcpy.env.snapRaster = Raster(snap)
    arcpy.env.extent = zone_lyr

    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_lyr, "Value", raster_lyr, temp)
    print "Completed Zonal Histogram"

    return temp


def zonal_hist(in_zone, in_value_raster, set_raster_symbology, region_c, use_name, temp_table, final_folder, snap,
               lst_cnt):
    # In paths
    path_fc, in_file = os.path.split(in_zone)
    file_name = in_file.split("_")[0]

    break_use = os.path.basename(use_path).split("_")
    break_bool = False
    use_nm_folder = region_c  # starting point that will be used for use_nm_folder

    for v in break_use:  # SEE TODO
        if v != region and v != 'CDL':
            # 'Area' and v != 'AK' and v != '2S'and v != '55N' and v != 'Area' and v != '4N' and v != '20N':
            pass
        else:
            break_bool = True
        if break_bool:
            if v == region_c:
                pass
            else:
                use_nm_folder = use_nm_folder + "_" + v

    use_nm_folder = use_nm_folder.split(".")[0]

    if not os.path.exists(final_folder + os.sep + use_nm_folder):
        os.mkdir(final_folder + os.sep + use_nm_folder)

    run_id = file_name + "_" + use_nm_folder + "_" + str(lst_cnt)
    out_path_final = final_folder

    csv = file_name + "_" + use_nm_folder + '.csv'
    if os.path.exists(out_path_final + os.sep + csv):
        print ("Already completed run for {0}".format(run_id))

    elif not os.path.exists(out_path_final + os.sep + csv):
        zone_time = datetime.datetime.now()

        print ("Running Statistics...for nhd file {0} and raster {1}".format(file_name, use_name))

        arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
        for ras in lst_cnt:
            arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
            temp_return = zone(ras, "rd_lyr", temp_table, snap)
            list_fields = [f.name for f in arcpy.ListFields(temp_return)]
            att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
            att_df = pd.DataFrame(data=att_array)
            att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
            att_df = att_df.T
            if lst_cnt.index(ras) == 0:
                work_df = att_df.copy()
            else:
                # drops the first two row which has the objID and LABEL; theses rows are already found in work_df
                att_df = att_df.drop(att_df.index[[0,1]])
                work_df = pd.concat([work_df,att_df], axis=0)
                work_df.to_csv(out_path_final + os.sep + csv)
        print 'Final file can be found at {0}'.format(out_path_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
arcpy.CheckOutExtension("Spatial")
create_directory(os.path.dirname(os.path.dirname(out_results)))
create_directory(os.path.dirname(out_results))
create_directory(out_results)
arcpy.env.workspace = in_location_nhd
list_raster = (arcpy.ListRasters())
count_sp = len(arcpy.ListRasters())
count = 0

for raster_in in list_raster:
    count += 1
    in_sp = in_location_nhd + os.sep + raster_in
    raster_array = arcpy.da.TableToNumPyArray(in_sp, ['Rowid','VALUE', 'COUNT'])
    raster_zone_df = pd.DataFrame(data=raster_array, dtype=object)
    max_row = raster_zone_df['Rowid'].max()
    split = max_row / split_by

    arcpy.MakeRasterLayer_management(in_sp, "raster_a", "Rowid> " + str(split))
    arcpy.MakeRasterLayer_management(in_sp, "raster_b", "Rowid <= " + str(split))
    print "\nWorking on uses for {0} NHD file {1} of {2}".format(raster_in, count, count_sp)
    for use_nm in use_list:
        out_folder = out_results

        if region != 'CONUS':
            snap_raster = snap_raster_dict[region]
            if use_location.endswith('UseLayers.gdb'):
                symbologyLayer = symbology_dict[region][0]
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            elif use_location.endswith('OnOffField.gdb'):
                symbologyLayer = symbology_dict[region][1]
                out_folder = out_folder + os.sep + 'OnOffField'
                create_directory(out_folder)
        else:
            snap_raster = snap_raster_dict[region]
            if use_location.endswith('UseLayers.gdb'):
                symbologyLayer = symbology_dict[region][0]
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            elif use_location.endswith('OnOffField.gdb'):
                symbologyLayer = symbology_dict[region][1]
                out_folder = out_folder + os.sep + 'OnOffField'
                create_directory(out_folder)
            elif use_location.endswith('Yearly.gdb'):
                symbologyLayer = symbology_dict[region][2]
                out_folder = out_folder + os.sep + 'Indiv_Year_raw'
                create_directory(out_folder)
        use_path = use_location + os.sep + use_nm
        print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)
        # try: uncomment try/expect loop if runs need to be done quickly: be sure to check if something fail and why
        zonal_hist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file, out_folder, snap_raster,
                   ["raster_a", "raster_b"])
        # except Exception as error:
        #     print(error.args[0])
        #     print "Failed on {0} with use {1}".format(raster_in, use_nm)
        current_use += 1
    current_use = 1


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)