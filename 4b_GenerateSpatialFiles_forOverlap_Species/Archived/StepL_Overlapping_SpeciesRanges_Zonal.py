import pandas as pd
import os
import arcpy
import datetime
import numpy as np

master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410.csv'
overlapping_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species' \
                    r'\overlapping_species'
completed_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species' \
                  r'\All_sp.csv'
species_table_stopped = 'r_amphib_r_clams.csv'
look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range' \
             r'\R_Clipped_Union_MAG_20161102.gdb'
look_up_raster = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Clipped_MaxArea.gdb'

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

arcpy.env.workspace = look_up_raster
list_raster = arcpy.ListRasters()

species_df = pd.read_csv(master_list)
list_sp = species_df['EntityID'].values.tolist()
sp_array = pd.DataFrame(columns=list_sp)
if os.path.exists(completed_table):
    sp_array = pd.read_csv(completed_table, dtype= object)
sp_array['EntityID'] = list_sp
list_sp.insert(0, 'EntityID')
sp_array = sp_array.reindex(columns=list_sp)
sp_array.fillna(0, inplace=True)
list_sp.remove('EntityID')


def raster_tables(in_spe, raster_row):
    raster_row['Value'] = raster_row['Value'].astype(str)
    raster_row['Values'] = raster_row['Value'].map(lambda x: x.replace('.0', ''))

    in_spe['ZoneID'] = in_spe['ZoneID'].astype(str)
    in_spe['ZoneID'] = in_spe['ZoneID'].map(lambda x: x.replace('.0', ''))

    col_species = raster_row['Value'].values.tolist()
    for value in col_species:
        spe_list = in_spe.loc[in_spe['ZoneID'] == str(value), 'ZoneSpecies']
        ents = (spe_list.iloc[0].replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace
                ("'", "")).split(',')
        ents = [j for j in ents if j in sp_array['EntityID'].values.tolist()]
        for entid in ents:
            for entid_col in ents:
                if entid != entid_col:
                    current_value = sp_array.loc[sp_array['EntityID'] == entid, entid_col].iloc[0]
                    count = raster_row.loc[raster_row['Value'] == value, 'Count'].iloc[0]
                    add_value = int(current_value) + int(count)
                    sp_array.loc[sp_array['EntityID'] == entid, entid_col] = add_value
                    sp_array.loc[sp_array['EntityID'] == entid_col, entid] = add_value
                else:
                    pass


def parse_tables(in_table, in_row_sp, in_col_spe):
    columns = in_table.columns.values.tolist()
    column_values = [col for col in columns if col.startswith('VALUE')]

    in_table['LABEL'] = in_table['LABEL'].map(lambda x: x.replace(',', '')).astype(str)
    in_table['sum'] = in_table[column_values].sum(axis=1)
    in_table = in_table.loc[in_table['sum'] != 0]
    row_values = in_table['LABEL'].values.tolist()

    in_row_sp['ZoneID'] = in_row_sp['ZoneID'].astype(str)
    in_row_sp['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace('.0', ''))

    in_col_spe['ZoneID'] = in_col_spe['ZoneID'].astype(str)
    in_col_spe['ZoneID'] = in_col_spe['ZoneID'].map(lambda x: x.replace('.0', ''))

    for value in row_values:
        if row_values.index(value) in np.arange(0, (len(row_values) + 1), 25):
            print '     Working on row zone ID {0}: completed {2} of {1}'.format(value, len(row_values),
                                                                                 row_values.index(value))
        spe_row_list = in_row_sp.loc[in_row_sp['ZoneID'] == str(value), 'ZoneSpecies']
        row_ents = (spe_row_list.iloc[0].replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace
                    ("'", "")).split(',')
        row_ents = [k for k in row_ents if k in sp_array['EntityID'].values.tolist()]
        col_header_zone = [col.split("_")[1] for col in column_values]
        for col in col_header_zone:
            spe_col_list = in_col_spe.loc[in_col_spe['ZoneID'] == col, 'ZoneSpecies']
            col_ents = (spe_col_list.iloc[0].replace('[', '').replace(']', '').replace('u', '').replace(' ', '').
                        replace("'", "")).split(',')
            col_ents = [l for l in col_ents if l in sp_array['EntityID'].values.tolist()]
            for entid_row in row_ents:
                for entid_col in col_ents:
                    current_value = sp_array.loc[sp_array['EntityID'] == entid_row, entid_col].iloc[0]
                    count = in_table.loc[in_table['LABEL'] == value, 'VALUE_' + col].iloc[0]
                    add_value = int(current_value) + int(count)
                    sp_array.loc[sp_array['EntityID'] == entid_row, entid_col] = add_value


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_folders = os.listdir(overlapping_table)
previously_run = False
for folder in list_folders:
    print 'Working on folder {0}: completed {2} of {1}'.format(folder, len(list_folders), list_folders.index(folder))
    in_raster = [v for v in list_raster if v.startswith('R_' + folder.split("_")[1].title())]
    in_fc = [v for v in list_fc if v.startswith('R_' + folder.split("_")[1].title())]
    raster_array = arcpy.da.TableToNumPyArray(look_up_raster + os.sep + in_raster[0], ['Value', 'Count'])
    fc_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_fc[0], ['ZoneID', 'ZoneSpecies'])
    raster_df = pd.DataFrame(data=raster_array)
    fc_df = pd.DataFrame(data=fc_array)
    raster_tables(fc_df, raster_df) # species in same sp group overlapping with each other

    list_csv_in = os.listdir(overlapping_table + os.sep + folder)
    list_csv = [csv for csv in list_csv_in if csv.endswith('.csv')]
    for csv in list_csv:
        if not previously_run:
            if csv != species_table_stopped:
                continue
            else:
                previously_run = True
        if previously_run:
            parse_nm = csv.split("_")
            col_sp = parse_nm[1]
            row_sp = parse_nm[3].replace('.csv', '')
            overlapping_df = pd.read_csv(overlapping_table + os.sep + folder + os.sep + csv, dtype=object)
            print "  {0}".format(csv)
            in_col = [v for v in list_fc if v.startswith('R_' + col_sp.title())]
            in_row = [v for v in list_fc if v.startswith('R_' + row_sp.title())]

            row_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_row[0], ['ZoneID', 'ZoneSpecies'])
            row_df = pd.DataFrame(data=row_array, dtype=object)

            col_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_col[0], ['ZoneID', 'ZoneSpecies'])
            col_df = pd.DataFrame(data=col_array, dtype=object)
            parse_tables(overlapping_df, row_df, col_df)

            sp_array.to_csv(r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range'
                            r'\Overlaping_species\All_sp.csv')
        else:
            pass

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
