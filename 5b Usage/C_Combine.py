import arcpy
from arcpy.sa import *
import datetime
import os

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
arcpy.CheckOutExtension("Spatial")

in_directory_species_grids = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\SpCompRaster_byProjection\Grids_byProjection'
use_layer_library = r'L:\Workspace\UseSites\ByProjection'
elevation_dict = {'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

habitat_dict = {'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

snap_dict = {'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}
out_directory = os.path.dirname(in_directory_species_grids) +os.sep+ 'Grid_byProjections_Adjustments'
if not os.path.exists(out_directory):
    os.mkdir(out_directory)

list_dir = os.listdir(in_directory_species_grids)

for folder in list_dir:
    region = folder.split('_')[0]
    # Set Snap Raster environment
    arcpy.env.snapRaster = snap_dict[region]
    out_directory = os.path.dirname(in_directory_species_grids) + os.sep + 'Grid_byProjections_Adjustments' + os.sep + folder
    if not os.path.exists(out_directory):
        os.mkdir(out_directory)
    arcpy.env.workspace = in_directory_species_grids + os.sep + folder
    sp_list = arcpy.ListRasters()
    for spe_raster in sp_list:
        on_off_field = use_layer_library + os.sep+region +'_'+ 'OnOffField.gdb'
        arcpy.env.workspace = on_off_field
        raster_list = arcpy.ListRasters()
        raster_list.append(elevation_dict[region])
        raster_list.append(habitat_dict[region])
        raster_list.append(in_directory_species_grids + os.sep +folder+ os.sep+spe_raster)
        # print raster_list
        outCombine = Combine(raster_list)
        outCombine.save(out_directory +os.sep+spe_raster)
        field = [f.name for f in arcpy.ListFields(out_directory +os.sep+spe_raster)]
        for f in [u'Rowid', u'VALUE', u'COUNT']:
            field.remove(f)

        print field

        if len(field) == len (raster_list):


            print raster_list
            for raster in raster_list:
                base_file = os.path.basename(raster)
                bool_pass = False
                col_header = region

                for v in base_file.split("_") :
                    if v != region and v!= 'ch':
                        pass
                    else:
                        bool_pass = True
                    if bool_pass:
                        if v == region:
                            pass
                        else:
                            col_header = col_header + "_" + v
                col_header = col_header.replace(region +'_','')
                if col_header.startswith('ch_') or col_header.startswith('r_'):
                    pass
                else:
                    arcpy.AddField_management(out_directory +os.sep+spe_raster, col_header, "LONG")
                    with arcpy.da.UpdateCursor(out_directory +os.sep+spe_raster,[field[raster_list.index(raster)], col_header]) as cursor:
                        for row in cursor:
                            row[1] = row[0]
                            cursor.updateRow(row)
        arcpy.BuildPyramids_management(out_directory + os.sep + spe_raster)
        print 'completed {0}'.format(out_directory + os.sep + spe_raster)



# where = '"VALUE" = 0'
# arcpy.MakeRasterLayer_management("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive"
#                                  "\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb"
#                                  "\Albers_Conical_Equal_Area_CONUS_CDL_1015_60x2_euc", "rdlayer_60", where)
#
# arcpy.MakeRasterLayer_management("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive"
#                                  "\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb"
#                                  "\Albers_Conical_Equal_Area_CONUS_CDL_1015_70x2_euc", "rdlayer_70", where)
#
# arcpy.MakeRasterLayer_management("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive"
#                                  "\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb"
#                                  "\Albers_Conical_Equal_Area_CONUS_Nurseries_euc", "rdlayer_nurse", where)
#
# AAoutCombine = Combine(["rdlayer_60", "rdlayer_70","rdlayer_nurse"])
# AAoutCombine.save("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA"
#                   "\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb\CONUS_AA_Diaz")
#
# # outCombine.save("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb\CONUS_Ag_Diaz")
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)