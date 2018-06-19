import arcpy
from arcpy import env
import os
import datetime
from arcpy.sa import *
import pandas as pd

# Description: Simplifies raster by replacing cells in a raster based on the majority of their contiguous neighboring
# cells then extracts zone made up of more than 10 pixels and runs the nibble tool to fill in holes.

# process for generalizing raster outline by ESRI at:
# http://desktop.arcgis.com/en/arcmap/10.3/tools/spatial-analyst-toolbox/generalization-of-classified-raster-imagery.htm

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
arcpy.CheckOutExtension("Spatial")

in_directory_species_grids = r'D:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection' \
                             r'\Grid_byProjections_Combined'
raster_layer_libraries = r'D:\Workspace\UseSites\ByProjection'
out_directory = os.path.dirname(in_directory_species_grids) + os.sep + 'Grid_byProjections_Combined_Smoothed'

skip_region = [ 'CONUS',  'AK', 'HI', 'AS','CNMI','GU', 'VI', 'PR',]
#skip_region = [ 'CONUS', 'CNMI','PR','VI', 'AK', 'GU']
#skip_species = ['r', 'ch', 'amphi', 'amphib', 'birds', 'clams', 'conife', 'crust', 'crusta', 'ferns', 'fishes', 'fishe',
#                'flower',
#                'flowe', 'mammal', 'mamma', 'reptil', 'repti', 'snails', 'snail']
skip_species = []
snap_dict = {'CONUS': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
             'HI': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
             'AK': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
             'AS': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
             'CNMI': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
             'GU': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
             'PR': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
             'VI': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

# create out directory if it does not exist
if not os.path.exists(out_directory):
    os.mkdir(out_directory)

list_dir = os.listdir(in_directory_species_grids)

for folder in list_dir:
    region = folder.split('_')[0]
    print(region)
    if region in skip_region:
        continue
    else:
        # Create out folder if it doesn't exist
        out_folder = out_directory + os.sep + folder
        out_folder_mf = out_directory + os.sep + 'temp' + os.sep + folder + '_MF'
        out_folder_extract = out_directory + os.sep + 'temp' + os.sep + folder + '_EA'
        out_folder_link = out_directory + os.sep + 'temp' + os.sep + folder + '_link'
        out_folder_nibble = out_directory + os.sep + 'temp' + os.sep + folder + '_nibble'
        out_folder_ext = out_directory + os.sep + 'temp' + os.sep + folder + '_ext'

        if not os.path.exists(out_folder):
            os.mkdir(out_folder)
        if not os.path.exists(out_directory + os.sep + 'temp'):
            os.mkdir(out_directory + os.sep + 'temp')

        if not os.path.exists(out_folder_mf):
            os.mkdir(out_folder_mf)
        if not os.path.exists(out_folder_extract):
            os.mkdir(out_folder_extract)
        if not os.path.exists(out_folder_link):
            os.mkdir(out_folder_link)
        if not os.path.exists(out_folder_nibble):
            os.mkdir(out_folder_nibble)
        if not os.path.exists(out_folder_ext):
            os.mkdir(out_folder_ext)
        arcpy.env.workspace = in_directory_species_grids + os.sep + folder
        sp_list = arcpy.ListRasters()
        print in_directory_species_grids + os.sep + folder
        print sp_list
        # combine each species file with the habitat and elevation rasters
        for spe_raster in sp_list:

            if spe_raster.split("_")[1] in skip_species:
                continue
            else:
                start_raster = datetime.datetime.now()
                if not arcpy.Exists(out_folder + os.sep + spe_raster):
                    in_spe = in_directory_species_grids + os.sep + folder + os.sep + spe_raster
                    print in_spe
                    arcpy.Delete_management("species")
                    arcpy.MakeRasterLayer_management(in_spe, "species")

                    my_extent = Raster(in_spe).extent
                    arcpy.env.extent = my_extent

                    # inset the path to the species file to list of raster to combine in index position 1
                    # Set Snap Raster environment
                    arcpy.Delete_management("snap")
                    arcpy.MakeRasterLayer_management(snap_dict[region], "snap")
                    arcpy.env.snapRaster = "snap"
                    # run combine : includes species raster , on/off field, habitat and elevation
                    print 'Start Majority Filter for {0}'.format(spe_raster)
                    # Execute MajorityFilter: Replaces cells in a raster based on the majority of their contiguous
                    # neighboring cells
                    outMajFilt = MajorityFilter(in_spe, "EIGHT", "HALF")

                    # Assigns region group so attribute tables can be link: For each cell in the output, the identity
                    # of the connected region to which that cell belongs is recorded. A unique number is assigned to
                    # each region
                    outRgnGrp = RegionGroup(outMajFilt, "EIGHT", "", "ADD_LINK", 0)
                    arcpy.BuildRasterAttributeTable_management(outRgnGrp)

                    # sql clause to remove zone represented only by a few pixels; makes layer with zone made up of more
                    # than 10 pixels
                    print 'Start ExtractByAttributes for {0}'.format(spe_raster)
                    inSQLClause = "COUNT > 10"
                    try:
                        attExtract = ExtractByAttributes(outRgnGrp, inSQLClause)
                        arcpy.BuildRasterAttributeTable_management(attExtract)

                        # runs nibble one extract file;  eliminates and replaces it with the closest cell with a value on
                        # the classified raster
                        print 'Start Nibble for {0}'.format(spe_raster)
                        nibbleOut = Nibble(outRgnGrp, attExtract, "DATA_ONLY")
                        arcpy.BuildRasterAttributeTable_management(nibbleOut)
                        # Saves Raster Layers
                        outRgnGrp.save(out_folder_link + os.sep + spe_raster)
                        outMajFilt.save(out_folder_mf + os.sep + spe_raster)
                        attExtract.save(out_folder_extract + os.sep + spe_raster)
                        nibbleOut.save(out_folder_nibble + os.sep + spe_raster)

                        # Export look up table based on attribute table so the original zone value from combination run
                        # is present

                        # Merge attribute table from nibble to the region group table using the value field - table will no
                        # have the LINK column from the region group; value in the LINK column is match the values in the
                        # VALUE column from the original table.  Copy values in the original attribute table to a new column
                        # with header 'LINK' then use this column to merge the information into the final attribute table
                        # table is then export to csv

                        list_fields_link = [f.name for f in arcpy.ListFields(out_folder_link + os.sep + spe_raster)]
                        att_array_link = arcpy.da.TableToNumPyArray(out_folder_link + os.sep + spe_raster, list_fields_link)
                        att_link = pd.DataFrame(data=att_array_link)
                        att_link.columns = [u'Rowid_link', u'VALUE', u'COUNT_link', u'LINK']

                        list_fields_out = [f.name for f in arcpy.ListFields(out_folder_nibble + os.sep + spe_raster)]
                        att_array_out = arcpy.da.TableToNumPyArray(out_folder_nibble+ os.sep + spe_raster, list_fields_out)
                        att_out = pd.DataFrame(data=att_array_out)
                        att_out.columns = [[u'Rowid_out', u'VALUE', u'COUNT_out']]
                        out_df = pd.merge(att_out, att_link, on='VALUE', how='left')


                        # list_fields_in = [f.name for f in arcpy.ListFields(in_spe)]
                        # att_array_in = arcpy.da.TableToNumPyArray(in_spe, list_fields_in)
                        att_df_org = pd.read_csv(in_directory_species_grids +os.sep+folder+ os.sep + spe_raster + '_att.csv')
                        col_header = pd.read_csv(in_directory_species_grids+os.sep+folder+os.sep+spe_raster+'_lookup_rasters.csv')
                        att_df_org['LINK'] = att_df_org['VALUE'].map(lambda x: x).astype(long)
                        out_col = []
                        filter_col =[]
                        for v in att_df_org.columns.values.tolist():
                            if v in [u'Rowid', u'VALUE', u'COUNT']:
                                out_col.append(v+"_in")
                                # filter_col.append(v+"_in")
                            elif v in col_header['Default output header'].values.tolist():
                                new_col = col_header.loc[col_header['Default output header'] == v, 'Desired output header'].iloc[0]
                                out_col.append(new_col)
                            else:
                                out_col.append(v)
                        att_df_org.columns = out_col

                        out_df = pd.merge(out_df, att_df_org, on='LINK', how='left')
                        # out_df = out_df[filter_col]
                        out_df.to_csv(out_folder_nibble + os.sep + spe_raster + '_att_smooth.csv')
                        arcpy.Delete_management("view")
                        # arcpy.Delete_management("view2")
                        arcpy.Delete_management('lyr')
                        arcpy.MakeTableView_management(out_folder_nibble + os.sep + spe_raster + '_att_smooth.csv', "view")
                        # arcpy.MakeTableView_management(in_directory_species_grids +os.sep+folder+ os.sep + spe_raster + '_att.csv', "view2")

                        arcpy.MakeRasterLayer_management(out_folder_nibble + os.sep + spe_raster, 'lyr')

                        arcpy.AddJoin_management('lyr', 'VALUE', "view", 'VALUE')
                        # arcpy.AddJoin_management('lyr', spe_raster + '_att_smooth.csv.LINK', "view2", 'VALUE')
                        where = '"{0}_att_smooth.csv.VALUE_in" IS NOT NULL'.format(spe_raster)
                        outExtract = ExtractByAttributes('lyr', where)
                        outExtract.save(out_folder_ext + os.sep + spe_raster)

                        print [f.name for f in arcpy.ListFields(out_folder_ext + os.sep + spe_raster)]
                        lookupfield = u'VALUE_IN'
                        lookupRaster = Lookup(outExtract,lookupfield)
                        lookupRaster.save(out_folder + os.sep + spe_raster)

                        # Execute Boundary clean if we want to do this
                        # print 'Start Boundary Clean for {0}'.format(spe_raster)
                        # OutBndCln = BoundaryClean(outMajFilt, "ASCEND", "TWO_WAY")

                        print 'Saved {0}, Build attribute table'.format(out_folder + os.sep + spe_raster)
                        arcpy.BuildRasterAttributeTable_management(out_folder + os.sep + spe_raster)

                        print 'Build pyramids'
                        arcpy.BuildPyramids_management(out_folder + os.sep + spe_raster)
                        print 'Completed {0} in {1}'.format(out_folder + os.sep + spe_raster, (datetime.datetime.now()
                                                                                               - start_raster))
                    except Exception as error:
                        print('Error was {0} elapse time {1}'.format(error.args[0], datetime.datetime.now() - start_raster))
                        if arcpy.Exists(out_folder + os.sep + spe_raster[:8]):
                            arcpy.Delete_management(out_folder + os.sep + spe_raster[:8])
                else:
                    print 'Already create file {0}'.format(out_folder + os.sep + spe_raster)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
