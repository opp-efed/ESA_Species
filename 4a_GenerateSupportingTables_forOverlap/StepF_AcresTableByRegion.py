import arcpy
import pandas as pd
import datetime

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title - generates acres table for all species in each region and full spatial file

# ASSUMPTIONS/Agreements  - ESA Team Fall 2016; updated Fall 2017
#       - Acres calculated for the whole range used web mercator
#       - Acres for a specific region uses the projection specified for that region - see UseList documentation
#       - Total acres on land is the sum of the individual region value; Total NL48 is the sum of the NL48 regions

# outpath and name of csv for acres tables
out_csv = 'D:\Species\Composites_Spring2020\RegionalFiles\CriticalHabitat\CH_Acres_by_region_20200427.csv'
# out table
# in GDB with projected comp files, regional and world projection for full spatial file; done for range and crithab
inGDB_list = [
    r'D:\Species\Composites_Spring2020\RegionalFiles\CriticalHabitat\CH_SpGroupComposite_ProjectedtRegion_20200427.gdb',
    r'D:\Species\Composites_Spring2020\RegionalFiles\CriticalHabitat\CH_WebMercator.gdb']
# current master for species info
master_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_June2020.csv"

# Colums in Master that should be included
col_included = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                'Source of Call final BE-Critical Habitat', 'Critical_Habitat_YesNo', 'Migratory', 'Migratory_YesNo',
                'CH_Filename', 'Range_Filename', 'L48/NL48']
# regional fc
regional_fc = r'D:\Species\Boundaries.gdb\Regions_dissolve'  # regional polygons
# header values that won't be added dynamically
acres_total_headers = ['EntityID', 'TotalAcres']


# # Functions
# extracts species info from table and loads it a species df
def extract_species_info(master_in_table, col_from_master):
    master_list_df = pd.read_csv(master_in_table)
    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    sp_info_header = sp_info_df.columns.values.tolist()
    return sp_info_df, sp_info_header,


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

main_out_header = []
main_species_infoDF, main_sp_header = extract_species_info(master_list, col_included)
main_out_header.extend(main_sp_header)

# Generates list of regions
with arcpy.da.SearchCursor(regional_fc, ['Region']) as cursor:
    region_list = sorted({row[0] for row in cursor})

acres_headers = []
acres_main = acres_total_headers
[acres_headers.append('Acres_' + str(region)) for region in region_list]

acres_main.extend(acres_headers)
# empty df to load acres into
df_empty_col = pd.DataFrame(columns=acres_main)
# merge species info df and acres df
df_Acres = pd.merge(main_species_infoDF, df_empty_col, on='EntityID', how='left')
# set EntityID to string
df_Acres['EntityID'] = df_Acres['EntityID'].astype(str)
# list of entid found in master
ent_list_master = df_Acres['EntityID'].values.tolist()

# loops through gdb and for comp files found in each gdb will do a row search by species and loads acres into df
# TODO Put into function
for gdb in inGDB_list:
    arcpy.env.workspace = gdb
    fc_list = arcpy.ListFeatureClasses()
    for fc in fc_list:
        print fc
        field_list = [f.name for f in arcpy.ListFields(fc) if not f.required]

        field_list.remove('EntityID')
        for v in field_list:
            if str(v) in acres_main:
                with arcpy.da.SearchCursor(fc, (['EntityID', str(v)])) as cursor:
                    for row in cursor:
                        entid = str(row[0])
                        acres_region = (row[1])
                        # extracts row index for DF using the index of the entid in the list of all entid included
                        entid_index = ent_list_master.index(entid)
                        # check the value in this row index of the EntityID col to make sure it matches the fc entid
                        # This number is coming out as s float even though assign and str type to this col
                        entid_check = df_Acres.loc[entid_index, 'EntityID']
                        # entid_check = (entid_check.split('.'))[0] #if entid being read in not as str
                        if entid_check == entid:
                            df_Acres.loc[entid_index, str(v)] = acres_region
                            # print 'Added {0} acres for species {1} in col {2}'.format(acres_region,entid,v)
                        else:
                            print 'Error EntityID check fail before adding acres for {0}'.format(entid)
            else:
                pass

# selects just the acres headers and sums them to a new column titled total acres on land

nl48_col = [v for v in acres_headers if not v.endswith('L48') or v.endswith('CONUS')]
print ('Columns included in the TotalAcresNL48 {0}'.format(nl48_col))
df_Acres['TotalAcresOnLand'] = df_Acres[acres_headers].sum(axis=1)
df_Acres['TotalAcresNL48'] = df_Acres[nl48_col].sum(axis=1)

# TODO Take this out when the region_dissolve regions are updated to CONUS
if 'Acres_L48' in df_Acres.columns.values.tolist():
    df_Acres.rename(columns = {'Acres_L48':'Acres_CONUS'}, inplace=True)
df_Acres.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
