import arcpy
import pandas as pd
import os
from arcpy.sa import *

arcpy.CheckOutExtension("Spatial")
# Purpose: if there is millions of unique combination after running the combine on the the species input files; this
# script will split the file into multiple input files with not more than 1 million unique combinaitons

in_location = r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2\Grid_byProjections_Combined' \
              r'\CONUS_Albers_Conical_Equal_Area'

species_files_to_split = ['r_birds', 'r_fishes' , 'r_flower','r_mammal' ]
outlocation =  r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2\Grid_byProjections_Combined' \
               r'\CONUS_Albers_Conical_Equal_Area\split_files'
nm_in_split = 100000

for v in species_files_to_split:
    in_spe_file = in_location +os.sep + v
    field_names = [f.name for f in arcpy.ListFields(in_spe_file)]
    if 'Rowid' in field_names:
        field_names.remove('Rowid')
    print field_names
    sp_zone_array = arcpy.da.TableToNumPyArray(in_spe_file, field_names)
    sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)
    print len(sp_zone_df), len(sp_zone_array)
    number_of_splits = float(len(sp_zone_df) )/nm_in_split
    split_values = nm_in_split
    previous_value = 0
    count = 1
    while count < number_of_splits:
        out_name = v[:6] + str(count)
        if not arcpy.Exists(outlocation +os.sep+out_name):
            where =''"\"VALUE\" <={0} AND \"VALUE\" >{1} AND \"ALBERS_CONICAL_1\" >{2} AND \"ALBERS_CONICAL_2\" >{2}"''.format(split_values,previous_value, 0)
            print where
            attExtract = ExtractByAttributes(in_spe_file,where)
            arcpy.env.extent = "MINOF"
            attExtract.save(outlocation +os.sep+out_name)
            arcpy.BuildPyramids_management(outlocation +os.sep+out_name)

        previous_value = split_values
        split_values = previous_value + nm_in_split
        count += 1
    #
    # sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)