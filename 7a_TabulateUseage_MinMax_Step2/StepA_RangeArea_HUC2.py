import arcpy
import pandas as pd
import os
import datetime


look_up_fc_ab = r'D:\Lookup_R_Clipped_Union_CntyInter_HUC2ABInter_20180612'
in_directory_grids = r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2' \
                     r'\Grids_byProjection'
out_path = r'L:\Workspace\StreamLine\ESA\Species_area_tables'

master_list = r'C:\Users\JConno02' \
              r'\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
# Columns from the master species list that should be included in the output tables
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

list_folder = os.listdir(in_directory_grids)
list_fc_ab = os.listdir(look_up_fc_ab)

for folder in list_folder:
    out_df  = pd.DataFrame()
    arcpy.env.workspace = (in_directory_grids + os.sep + folder )
    list_species = arcpy.ListRasters()
    print list_species
    for species in list_species:
        lookup_csv = [t for t in list_fc_ab if t.startswith(species.split("_")[0].upper()
                                                            + "_" + species.split("_")[1].capitalize())]
        lookup_df = pd.read_csv(look_up_fc_ab + os.sep + lookup_csv[0], dtype=object)
        if not arcpy.Raster(in_directory_grids + os.sep + folder +os.sep+species).hasRAT:
            arcpy.BuildRasterAttributeTable_management(in_directory_grids + os.sep + folder +os.sep+species)
        par_zone_array = arcpy.da.TableToNumPyArray(in_directory_grids + os.sep + folder +os.sep+species,
                                                ['VALUE','COUNT'])

        par_zone_df = pd.DataFrame(data=par_zone_array, dtype=object)
        par_zone_df['VALUE'] = par_zone_df['VALUE'].map(lambda n: str(n).split('.')[0]).astype(str)
        lookup_df['HUCID'] = lookup_df['HUCID'].map(lambda n: str(n).split('.')[0]).astype(str)
        merge_huc = pd.merge(par_zone_df,lookup_df, how ='outer',left_on='VALUE', right_on='HUCID')
        copy_df = merge_huc[['EntityID', 'HUC2_AB','COUNT','STUSPS']].copy()
        copy_df = copy_df.loc[copy_df['COUNT'] > 0]

        df_sum = copy_df.groupby(['EntityID', 'HUC2_AB', 'STUSPS'], as_index=False).sum().reset_index()
        out_df = pd.concat([out_df,df_sum])

    out_df.to_csv(out_path + os.sep + folder+  "_HUC2.csv")
    print ("Exported {0}".format(out_path + os.sep + folder+ "_HUC2.csv"))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
