import arcpy
from arcpy.sa import *
import datetime
import os
import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Combines the species rasters with the habitat and elevation rasters used for usage species files generate
# used 4b - steps 4-6; used for overlap accounting for habitat and elevation

# Note Note: Must set the number of unique values in arcmap or arccatalog to 2,000,000,000 or more- under options see
# SOP for additional details

# User inputs
# folder with input species rasters in GRID Format
in_dir_species_grids = r'E:\Workspace\StreamLine\ESA\UnionFiles_Summer2019\Range\SpComp_UsageHUCAB_byProjection\Grids_byProjection'

# folder with use layer - this script will combine species with habitat,elevation, and on/off field
raster_layer_libraries = r'E:\Workspace\StreamLine\ByProjection'

# True - include on/off field False only include habitat and elevation
include_on_off = False

# out location
out_dir = os.path.dirname(in_dir_species_grids) + os.sep + 'Grid_byProjections_Combined_QC'

# regions to skip - use to run multiple instances ['AK','AS','CNMI','CONUS', HI','GU', 'PR','VI']
skip_region = ['AK', 'AS', 'CONUS', 'HI','VI', 'GU', 'CNMI']

# Species to skip spe abb from input species raster without the r or ch - use to run multiple instances
# include the species file name flag (r or ch) in list if running multiple instances so the temp files are skipped
skip_species = ['r', 'ch']

# snap rasters by region
snap_dict = {'CONUS': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
             'HI': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
             'AK': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
             'AS': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
             'CNMI': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
             'GU': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
             'PR': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
             'VI': r'E:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}


def set_extent(region, snap_rasters):  # set processing extent
    # set snap raster and processing extent (equal to snap raster)
    snap_raster = Raster(snap_rasters[region])  # snap raster used for processing extent - extract for region
    print ('Snap raster will be:  {0}'.format(snap_rasters[region]))  # print for tracking
    # Set Snap Raster environment
    arcpy.Delete_management("snap")  # deletes in memory layer from previous loop
    arcpy.MakeRasterLayer_management(snap_rasters[region], "snap")  # makes raster layer
    arcpy.env.snapRaster = "snap"  # set snap raster in the environmental variables
    # Set the processing extent to be equal to the snap raster; only species within the extent will be
    # included in the output species file
    my_extent = snap_raster.extent
    arcpy.env.extent = my_extent


def get_hab_ele_rast_path(region, raster_layers):
    # sets path to the elevation and habitat layers
    elevation_habitat = raster_layers + os.sep + region + '_' + 'HabitatElevation.gdb'
    arcpy.env.workspace = elevation_habitat  # set workspace as the location of ele/hab gdb
    # get list of the habitat and elevation files for region
    raster_list_elev_habitat = arcpy.ListRasters()
    raster_list_combine = [elevation_habitat+ os.sep + v for v in raster_list_elev_habitat]

    # UNCOMMENT FOR CONUS to remove impervious from list
    if region == 'CONUS':
        raster_list_combine.remove(u'E:\\Workspace\\StreamLine\\ByProjection\\CONUS_HabitatElevation.gdb\\Albers_Conical_Equal_Area_CONUS_nlcd_2011_impervious_2011_edition_2014_10_10')
    # raster_list_combine = [
    #     u'E:\\Workspace\\StreamLine\\ByProjection\\CONUS_HabitatElevation.gdb'
    #     u'\\Albers_Conical_Equal_Area_CONUS_dem_sm10',
    #     u'E:\\Workspace\\StreamLine\\ByProjection\\CONUS_HabitatElevation.gdb'
    #     u'\\Albers_Conical_Equal_Area_CONUS_gap_landfire_nationalterrestrialecosystems2011']

    return raster_list_combine  # return list of raster to include in combine


def get_on_off_field(raster_list_combine, region, raster_layers):
    # sets path to the on/off field layers for region - added boolean switch to turn, but may add it back
    on_off_field = raster_layers + os.sep + region + '_' + 'OnOffField.gdb'
    arcpy.env.workspace = on_off_field  # set workspace as the location of on/off gdb
    # get list of the on/off files for region
    raster_list_on_off = arcpy.ListRasters()
    # add path to the raster name for combine because the workspace is changing
    # append the habitat and elevation raster to to lst of raster to be combine with the path to the gdb
    # because the workspace is changing
    for v in raster_list_on_off:
        raster_list_combine.append(on_off_field + os.sep + v)
    return raster_list_combine  # return list of raster to include in combine


def run_combine(in_directory_species_grids, raster_list_combine, folder, out_folder, region):
    # sets workspace to locations with input species rasters and generates list
    arcpy.env.workspace = in_directory_species_grids + os.sep + folder
    sp_list = arcpy.ListRasters()
    # combine each species file with the habitat and elevation rasters
    for spe_raster in sp_list:  # loops over each species
        c_raster_list = raster_list_combine  # makes a copy of list with the use layers for combine
        if spe_raster.split("_")[1] in skip_species:  # bypasses species in the species skip list set by user
            continue
        else:
            print ('Start Generating species overlap files')  # print for tracking
            start_raster = datetime.datetime.now()  # elapse clock for species combine
            # path species file
            in_spe = in_directory_species_grids + os.sep + folder + os.sep + spe_raster
            c_raster_list.insert(0, in_spe)
            try:  # try except loop so an error won't cause the script to stop
                if not arcpy.Exists(out_folder + os.sep + spe_raster):  # skips files that already exist
                    # inset the path to the species file to list of raster to combine in index position 0
                    print ('Start Combine for {0} with {1}'.format(spe_raster, c_raster_list))  # print for tracking
                    print("Out location will be {0}".format(os.path.join(out_folder, spe_raster[:8])))
                    out_combine = Combine(c_raster_list)  # runs combine tools from spatial analyst
                    # saves combine; set out name to 8 character to the limit for a ESRI grid
                    out_combine.save(os.path.join(out_folder, spe_raster[:8]))
                    # print for tracking
                    print 'Saved {0} \n Build raster attribute table'.format(out_folder + os.sep + spe_raster[:8])
                    #  Builds raster attribute table and pyramids
                    arcpy.BuildRasterAttributeTable_management(os.path.join(out_folder, spe_raster[:8]))
                    print 'Build pyramids'
                    arcpy.BuildPyramids_management(out_folder + os.sep + spe_raster[:8])
                #  generates output text files used as lookups to identify habitat, elevation and species

                output_lookup_tables(spe_raster, c_raster_list, out_folder, region)
                print 'Completed {0} in {1}'.format(out_folder + os.sep + spe_raster[:8], datetime.datetime.now()
                                                    - start_raster)
            except Exception as error:  # exceptions for if error trips; print error and deleted any temp file generated
                print('Error was {0} elapse time was {1}'.format(error.args[0], datetime.datetime.now() - start_raster))
                if arcpy.Exists(out_folder + os.sep + spe_raster[:8]):
                    arcpy.Delete_management(out_folder + os.sep + spe_raster[:8])
        c_raster_list.remove(in_spe)


def output_lookup_tables(spe_raster, raster_list_combine, out_folder, region):
    # confirms file was not previously created
    print out_folder + os.sep + spe_raster[:8] + '_lookup_rasters.csv'
    if not os.path.exists(out_folder + os.sep + spe_raster[:8] + '_lookup_rasters.csv'):
        print ('Start output tables for {0}'.format(spe_raster))  # print for tracking
        # adds meaningful col headers for raster as the default will all start with the projection
        field = [f.name for f in arcpy.ListFields(out_folder + os.sep + spe_raster[:8])]

        for f in [u'Rowid', u'VALUE', u'COUNT']:  # remove columns that won't be update from field list
            field.remove(f)

        # Empty list to be use as placeholders to update columns names
        current_header = []
        desired_header = []
        path_to_raster = []

        if len(field) == len(raster_list_combine):  # confirms filed number and number of rasters is equal
            out_df = pd.DataFrame(index=(list(range(0, 10))))  # empty df to store look values  10 rows
            for raster in raster_list_combine:  # loops on the rasters in combine
                current_header.append(field[raster_list_combine.index(raster)])  # current header for col for raster
                path_to_raster.append(raster)  # path to raster of the column in loop
                base_file = os.path.basename(raster)  # name raster
                bool_pass = False  # boolean for loop when updated columns
                col_header = region  # base for final col header
                for v in base_file.split("_"):  # dynamically set up final col header based on input data
                    if v != region and v != 'ch' and v != 'R':
                        pass
                    else:
                        bool_pass = True
                    if bool_pass:
                        if v == region:
                            pass
                        else:
                            col_header = col_header + "_" + v
                col_header = col_header.replace(region + '_', '')
                # if the col header starts with the species flag then the col header is equal to the species file name
                if col_header.startswith('CH_') or col_header.startswith('R_') or col_header == region:
                    col_header = spe_raster
                desired_header.append(col_header)

            # all columns need to 10 rows - makes additional rows with value none
            merge_list_c = current_header + ([None] * (10 - len(current_header)))
            merge_list_d = desired_header + ([None] * (10 - len(desired_header)))
            merge_list_p = path_to_raster + ([None] * (10 - len(path_to_raster)))
            # saves results as columns in the output dataframe then exports to dataframe to a csv
            out_df['Default output header'] = pd.Series(merge_list_c).values
            out_df['Desired output header'] = pd.Series(merge_list_d).values
            out_df['Path to original raster'] = pd.Series(merge_list_p).values
            out_df.to_csv(out_folder + os.sep + spe_raster[:8] + '_lookup_rasters.csv')

            # when exporting to array cols after the rowid, count and value are 0. Export using the table
            # conversion then will join table back to the '_lookup_rasters.csv' exported above to get
            # meaningful columns
            # outputs the raster attribute table to a csv
            arcpy.TableToTable_conversion(out_folder + os.sep + spe_raster[:8], out_folder,
                                          spe_raster[:8] + '_att.csv')


def main(out_directory, in_directory_species_grids, skip_reg):
    start_time = datetime.datetime.now()  # elapse clock
    print "Start Time: " + start_time.ctime()
    arcpy.CheckOutExtension("Spatial")  # Check out license
    # create out directory if it does not exist
    if not os.path.exists(out_directory):
        os.mkdir(out_directory)
    list_dir = os.listdir(in_directory_species_grids)  # get list of folders with species files - starts with region
    for folder in list_dir:
        region = folder.split('_')[0]  # extract region abb
        print("\nWorking on {0}".format(region))   # print for tracking
        if region in skip_reg:  # bypass region is in skip region set by user
            continue
        else:
            # Create out folder if it doesn't exist
            out_folder = out_directory + os.sep + folder
            if not os.path.exists(out_folder):
                os.mkdir(out_folder)
            set_extent(region, snap_dict)  # set processing extent
            print('Generating lists of rasters to combine')  # print for tracking
            if include_on_off:  # user defined inputs - if on/off is should be included variable will be true
                # NOTE in CONUS when including on/off the number of unique values is extremely high
                hab_to_combine = get_hab_ele_rast_path(region, raster_layer_libraries)
                rast_to_combine = get_on_off_field(hab_to_combine, region, raster_layer_libraries)
            else:
                rast_to_combine = get_hab_ele_rast_path(region, raster_layer_libraries)
            for r in rast_to_combine:  # Build att table and pyramids for each raster in combine
                print ('Building attribute tables and pyramids for {0}'.format(r))
                arcpy.BuildRasterAttributeTable_management(r)
                arcpy.BuildPyramids_management(r)
            # Run combine for each species get list of species raster for region
            run_combine(in_directory_species_grids, rast_to_combine, folder, out_folder, region)
            end = datetime.datetime.now()  # end elapse clock
            print "End Time: " + end.ctime()
            elapsed = end - start_time
            print "Elapsed  Time: " + str(elapsed)


main(out_dir, in_dir_species_grids, skip_region)
