import pandas as pd
import os
import datetime
import arcpy

in_table = r'L:\Workspace\UseSites\ByProject\Overlapping_Use\Overlapping_Use_Matrix_acres.csv'
in_gdb = 'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb'
out_csv = r'L:\Workspace\UseSites\ByProject\Overlapping_Use\Overlapping_Use_Matrix_percent.csv'

useLookup = {'10x2': 'Corn',
             '20x2': 'Cotton',
             '30x2': 'Rice',
             '40x2': 'Soybeans',
             '50x2': 'Wheat',
             '60x2': 'Vegetables and Ground Fruit',
             '70x2': 'Orchards and Vineyards',
             '80x2': 'Other Grains',
             '90x2': 'Other RowCrops',
             '100x2': 'Other Crops',
             '110x2': 'Pasture',
             'CattleEarTag': 'Cattle Eartag',
             'Developed': 'Developed',
             'ManagedForests': 'Managed Forest',
             'Nurseries': 'Nurseries',
             'OSD': 'Open Space Developed',
             'ROW': 'Right of Way',
             'CullPiles': 'Cull Piles',
             #'Cultivated': 'Cultivated',
             #'NonCultivated': 'Non Cultivated',
             'PineSeedOrchards': 'Pineseed Orchards',
             'XmasTrees': 'Christmas Tree',
             'Diazinon': 'Diazinon_AA',
             #'Carbaryl': 'Carbaryl_AA',
             'Chlorpyrifos': 'Chlorpyrifos_AA',
             #'Methomyl': 'Methomyl_AA',
             'Malathion': 'Malathion_AA',
             'usa': 'Golf Courses',
             #'bermudagrass2': 'Bermuda Grass'
             }
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = in_gdb
use_raster = arcpy.ListRasters()
use_raster.remove('zAlbers_Conical_Equal_Area_CONUS_CDL_1015_30x2_euc')
list_use_raw = useLookup.keys()
final_uses = sorted(useLookup.values())
out_matrix_percent = pd.DataFrame(index=final_uses, columns=final_uses)
in_acres_df = pd.read_csv(in_table)
in_acres_df = in_acres_df.fillna(0)

for use in list_use_raw:
    colheader = useLookup[use]
    for raster in use_raster:
        parse_name = raster.split("_")
        if use in parse_name:
            in_raster = in_gdb + os.sep + raster
            att_array = arcpy.da.TableToNumPyArray((in_raster), ['Value','Count'])
            att_df = pd.DataFrame(data=att_array)
            pixel_count_use = int(att_df.ix[0,'Count'])
            print pixel_count_use
            msq_use = int(pixel_count_use)*(900)
            acres_use = int(msq_use)*(0.000247)
            print acres_use
            in_col =in_acres_df[str(colheader)]
            incol = in_col.fillna(0)
            print in_col
            outdf = (in_col/acres_use)*100
            out_matrix_percent[colheader] =outdf.values
            #TODO add outdf as the col with current heade rin out df
            print'test',outdf
        else:
            pass


print out_matrix_percent

# out_df = acres_overlap.round(0)
out_matrix_percent.to_csv(out_csv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
