import datetime
import os
import arcpy

import pandas as pd

# Author J.Connolly USEPA

# Description: Generates acres table for species using the zone found in the species raster based union composites
# rather than the vector file input

# This script has been approved for release by the U.S. Environmental Protection Agency (USEPA). Although
# the script has been subjected to rigorous review, the USEPA reserves the right to update the script as needed
# pursuant to further analysis and review. No warranty, expressed or implied, is made by the USEPA or the U.S.
# Government as to the functionality of the script and related material nor shall the fact of release constitute
# any such warranty. Furthermore, the script is released on condition that neither the USEPA nor the U.S. Government
# shall be held liable for any damages resulting from its authorized or unauthorized use.


# Notes-
#       1)  Files with high edge effect may result in different acres values using the different format, using this
#           table rather than the one for the vector file will keep the calculation consistent
#       2) Note- if vector files were used for overlap ArcGIS would do a temporary conversion to raster resulting in
#           same difference seen with the composites.


# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS
# Species group is found in index position 1 of all input result tables when split by '_'
# All raster are 30 meter cells - note previously VI and CNMI has some uses with a different cell size

# User input
# folder with species files; this can be range or critical habitat; ESRI GRIDS
in_raster_location = r"D:\Species\UnionFile_Spring2020\Range\SpComp_UsageHUCAB_byProjection\Grids_byProjection"
# Unioned FC
look_up_fc = r"D:\Species\UnionFile_Spring2020\Range\Lookup_R_Clipped_Union_CntyInter_HUC2ABInter_20200427"
look_up_value = 'HUCID'
outpath= r'D:\Species\UnionFile_Spring2020\CriticalHabitat\Acres_Pixels'
# ###############user input variables
master_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_June2020.csv"

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_YesNo', 'Migratory', 'Migratory_YesNo',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


# Static variables
list_folder = os.listdir(in_raster_location)

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


list_fc = os.listdir(look_up_fc)
find_file_type = in_raster_location.split(os.sep)

if 'Range' in find_file_type:
    file_type = "R_"
else:
    file_type = "CH_"

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

if not os.path.exists(outpath):
    os.mkdir(outpath)


# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


def parse_tables(in_table, in_row_sp):

    in_row_sp[look_up_value] = in_row_sp[look_up_value].map(lambda x: str(x).split('.')[0]).astype(str)
    # joins raster attribute table with the lookup table with the species entityIS
    merged_df = pd.merge(in_table, in_row_sp, on=look_up_value, how='outer')

    sum_sp= merged_df[['COUNT','EntityID']].copy()
    # groups by entityid and sums
    sum_sp= sum_sp.groupby('EntityID').sum().reset_index()
    # selects the species that occur in the current regions - these will have a pixel count valuw >= 0
    sum_sp_region = sum_sp[sum_sp['COUNT']>=0]

    return sum_sp_region


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

out_df = base_sp_df.copy()
for folder in list_folder:
    arcpy.env.workspace = in_raster_location + os.sep + folder
    outlocation = outpath + os.sep + folder
    list_raster = arcpy.ListRasters()
    create_directory(outlocation)
    region = folder.split("_")[0]
    for raster in list_raster:
        if os.path.exists(outlocation+os.sep+raster+date+'.csv'):
            use_array = pd.read_csv(outlocation+os.sep+raster+date+'.csv')
        else:
            my_raster_layer = arcpy.Raster(in_raster_location + os.sep + folder + os.sep + raster)
            if not my_raster_layer.hasRAT:
                print ("Building attribute table: {0}".format(in_raster_location + os.sep + folder + os.sep + raster))
                arcpy.BuildRasterAttributeTable_management(my_raster_layer, "Overwrite")

            raster_array = arcpy.da.TableToNumPyArray(in_raster_location + os.sep + folder + os.sep +
                                                      raster, ['VALUE', 'COUNT'])
            raster_zone_df = pd.DataFrame(data=raster_array, dtype=object)
            raster_zone_df[look_up_value] = raster_zone_df['VALUE'].map(lambda x: str(x).split('.')[0]).astype(str)
            sp_zone_fc = [j for j in list_fc if j.startswith(file_type + raster.split("_")[1].title())]
            sp_zone_df = pd.read_csv(look_up_fc + os.sep + sp_zone_fc[0])
            print("Working on: {0} -  region {2}; raster {1}".format(sp_zone_fc[0], raster, folder))

            use_array = parse_tables(raster_zone_df, sp_zone_df)
            use_array['MSQ'] = use_array['COUNT'].map(lambda x: int(x) * 900)
            use_array['Acres_' + region] = use_array['MSQ'].map(lambda x: int(x) * 0.000247)
            use_array.to_csv(outlocation+os.sep+raster+"_"+date+'.csv')

        use_array['EntityID'] = use_array['EntityID'].map(lambda x: str(x).split('.')[0]).astype(str)

        out_col = use_array.ix[:, ['EntityID', 'Acres_' + region]]
        out_col.to_csv(outlocation+os.sep+raster+'.csv')
        out_df = pd.merge(out_df, out_col, on='EntityID', how='left')
        out_df.to_csv(outlocation+os.sep+raster+'.csv')


out_df.to_csv(outpath+os.sep+file_type+'Temp_Acres_Pixels_'+date+'.csv')
out_df = pd.read_csv(outpath+os.sep+file_type+'Temp_Acres_Pixels_'+date+'.csv')
clean_df = out_df.copy()

# VALUES INFLATED HERE; RUN SCRIPT TWICE
acres_headers = [v for v in clean_df.columns.values.tolist() if 'Acres' in v.split("_")]
non_acres = [v for v in clean_df.columns.values.tolist() if v not in acres_headers]
acres_df = clean_df.ix[:, acres_headers]
non_acres_df = clean_df.ix[:, non_acres]
sum_df = acres_df.T.groupby([s.split("_")[0]+"_"+s.split("_")[1]for s in acres_df.T.index.values]).sum().T
acres_col = sum_df.columns.values.tolist()
nl48_col = [v for v in acres_col if not v.endswith('CONUS')]
concat_df = pd.concat([non_acres_df, sum_df], axis=1)

concat_df['TotalAcresOnLand'] = concat_df[acres_col].sum(axis=1)
concat_df['TotalAcresNL48'] = concat_df[nl48_col].sum(axis=1)
concat_df.to_csv(outpath+os.sep+file_type+'Acres_Pixels_'+date+'.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
