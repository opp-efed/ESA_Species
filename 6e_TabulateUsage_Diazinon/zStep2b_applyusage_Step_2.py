import pandas as pd
import os
import datetime

chemical_name = 'Diazinon'
region = 'CONUS'
in_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentResults\tabulated_usage_byspecies_clipped\L48\Range\Agg_Layers'
out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentChemTables\Diazinon\ApplyUsage'
county_usage = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               '\_ExternalDrive\_CurrentSupportingTables\Usage\Step2_Usage_Diaz_cnty.csv'
state_usage = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              '\_ExternalDrive\_CurrentSupportingTables\Usage\Step2_Usage_Diaz_state_2.csv'

look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentResults\Results_diaz\Diaz_RangeUses_lookup.csv'

master_list = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSupportingTables\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename','Range_Filename']

on_off_species =[]
in_acres_list = [
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Tables\CH_Acres_by_region_20170208.csv',
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Tables\R_Acres_by_region_20171204.csv']


def apply_usage(df, series):
    df_cols = df.columns.values.tolist()
    transform_df = df.T
    cols_t = transform_df.columns.values.tolist()
    transform_df = transform_df.apply(pd.to_numeric, errors='coerce')
    transform_df['factor'] = series.values
    transform_df[cols_t] = transform_df[cols_t].multiply(transform_df['factor'], axis=0)
    transform_df.drop('factor', axis=1, inplace=True)
    out_df = transform_df.T
    out_df.columns = df_cols
    return out_df


def calculation(type_fc, in_sum_df, cell_size, c_region, percent_type):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.select_dtypes(include=['number']).columns.values.tolist()
    if percent_type:
        acres_col = 'Acres_' + str(c_region)
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    else:
        acres_col = 'TotalAcresOnLand'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]

    if type_fc == "Raster":
        msq_conversion = cell_size * cell_size
        # convert pixels to msq
        overlap = in_sum_df.copy()
        overlap.ix[:, use_cols] *= msq_conversion
        # convert msq to acres
        overlap.ix[:, use_cols] *= 0.000247

        # generate percent overlap by taking acres of use divided by total acres of the species range
        overlap[use_cols] = overlap[use_cols].div(overlap[acres_col], axis=0)
        overlap.ix[:, use_cols] *= 100
        # Drop excess acres col- both regional and full range are included on input df; user defined parameter
        # percent_type determines which one is used in overlap calculation
        if percent_type:
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)
        else:
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)
        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print "ERROR ERROR"


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)



def on_off_field(row, cols, df):
    ent_id = row['EntityID']
    if ent_id in on_off_species:
        col = [v for v in cols if v.endswith("_0")]
        direct_over = row[col[0]]
        df.loc[df['EntityID'] == ent_id , [col[0]]]= 0
        for other_col in cols:
            if other_col == col[0]:
                pass
            else:
                value = row[other_col]
                df.loc[df['EntityID'] == ent_id , [other_col]]= value - direct_over
    else:
        pass

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

find_file_type = in_location.split(os.sep)

if 'Range' in find_file_type or 'range' in find_file_type:
    species_file_type = 'Range'
else:
    species_file_type = 'CriticalHabitat'

in_acres_table = ''
for v in in_acres_list:
    path, tail = os.path.split(v)
    if species_file_type == 'Range' and (tail[0] == 'R' or tail[0] == 'r'):
        in_acres_table = v
    elif species_file_type == 'CH' and (tail[0] == 'C' or tail[0] == 'c'):
        in_acres_table = v
    else:
        pass

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
out_df = species_df.loc[:, col_include_output]
use_lookup = pd.read_csv(look_up_use)
list_uses = os.listdir(in_location)
acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].astype(str)

st_usage_df = pd.read_csv(state_usage)
st_reindex_cols = st_usage_df.columns.values.tolist()
st_reindex_cols = [str(v) if len(v) == 2 or v == 'use name'else '0' + str(v) for v in st_reindex_cols]
st_reindex_cols.remove('use name')
st_reindex_cols.insert(0, 'EntityID')
out_location_sum = out_location + os.sep + 'States' + os.sep + 'SumSpecies'
out_folder_percent = out_location + os.sep + 'States' + os.sep + 'PercentOverlap_FullRange'
out_folder_percent_region = out_location + os.sep + 'States' + os.sep + 'PercentOverlap_RegionRange'
create_directory(os.path.dirname(out_location_sum))
create_directory(out_location_sum)
create_directory(out_folder_percent)
create_directory(out_folder_percent_region)

for folder in list_uses:
    print folder
    type_use = use_lookup.loc[use_lookup['FullName'] == folder.replace('_euc', ''), 'Included AA'].iloc[0]
    type_aa = use_lookup.loc[use_lookup['FullName'] == folder.replace('_euc', ''), 'Action Area'].iloc[0]

    use_nm = use_lookup.loc[use_lookup['FullName'] == folder.replace('_euc', ''), 'FinalUseHeader'].iloc[0]

    if type_use == 'x' or type_aa == 'x':
        st_vector = st_usage_df.loc[st_usage_df['use name'] == use_nm]
        st_vector = (st_vector.loc[:, st_vector.columns != 'use name'].astype(float)).values.tolist()

        se_st = pd.Series(st_vector[0])


        csv_list_st = os.listdir(in_location + os.sep + folder + os.sep + 'States')
        csv_list = [csv for csv in csv_list_st if csv.endswith('.csv')]

        for csv in csv_list:
            use_table = csv.replace('.csv', '')
            dis_type_check = use_table.split("_")
            if 'aerial' in dis_type_check:
                dis_type = 'aerial'
            elif 'ground' in dis_type_check:
                dis_type = 'ground'
            else:
                dis_type = 'direct'

            use_col = use_nm + "_" + dis_type

            raw_df = pd.read_csv(in_location + os.sep + folder + os.sep + 'States' + os.sep + csv)
            [raw_df.drop(m, axis=1, inplace=True) for m in raw_df.columns.values.tolist() if m.startswith('Unnamed')]
            raw_df = raw_df.reindex(columns=st_reindex_cols)
            col_use = list(st_reindex_cols)
            col_use.remove('EntityID')
            ent_id = raw_df['EntityID'].values.tolist()
            se_entid = pd.Series(ent_id)
            raw_df.drop('EntityID', axis=1, inplace=True)

            usage_df = apply_usage(raw_df, se_st)
            usage_df.insert(loc=0, column='EntityID', value=se_entid)
            usage_df[use_col] = usage_df[col_use].sum(axis=1)
            use_df = usage_df.ix[:,['EntityID',use_col]]
            out_df = pd.merge(out_df , use_df , on='EntityID', how='left')


    out_df.fillna(0,inplace=True)
    out_df.to_csv(out_location_sum + os.sep + 'Step2_usage_' + chemical_name + "_" + date + '.csv')

    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
    sum_df_acres = pd.merge(out_df, acres_for_calc, on='EntityID', how='left')

    # STEP 2: Run percent overlap for both the full range and the regional; set up acres table to include only
    # species found on the current use table (use_array)
    # TODO IN ON OFF FIELD removal and acres updated - should all Ag area be removed using culivated
    # layer and applying to acres tables, then the ag overlaps can just be made zero

    type_use = 'Raster'
    r_cell_size = 30

    full_range_overlap_out = out_folder_percent + os.sep + 'Step2_usage_' + chemical_name + "_" + date + '.csv'
    regional_range_overlap_out = out_folder_percent_region+ os.sep + 'Step2_usage_' + chemical_name + "_" + date + '.csv'

    print '       Generating full range percent overlap...species group:{0}'.format(csv.split('_')[1])
    percent_overlap_df_full = calculation(type_use, sum_df_acres, r_cell_size, region, False)
    [percent_overlap_df_full.drop(m, axis=1, inplace=True) for m in percent_overlap_df_full.columns.values.tolist() if
     m.startswith('Unnamed')]
    percent_overlap_df_full.to_csv(full_range_overlap_out)

    print '       Generating regional range percent overlap...species group:{0}'.format(csv.split('_')[1])
    percent_overlap_df_region = calculation(type_use, sum_df_acres, r_cell_size, region, True)
    [percent_overlap_df_region.drop(m, axis=1, inplace=True) for m in percent_overlap_df_region.columns.values.tolist() if
     m.startswith('Unnamed')]
    percent_overlap_df_region.to_csv(regional_range_overlap_out)


# cnty_usage_df = pd.read_csv(county_usage)
# cnty_reindex_cols = cnty_usage_df.columns.values.tolist()
# cnty_reindex_cols.remove('use name')
# cnty_reindex_cols.insert(0,'EntityID')


# cnty_vector = cnty_usage_df.loc[cnty_usage_df['use name'] == use_nm]
# cnty_vector = cnty_vector.loc[:, cnty_vector.columns != 'use name'].values.tolist()
# se_cnty = pd.Series(cnty_vector[0])
# csv_list_cnty = os.listdir(in_location+os.sep+folder+os.sep+'Counties'+os.sep+'SumSpecies')
# for csv in csv_list_cnty:
#     raw_df = pd.read_csv(in_location+os.sep+folder+os.sep+'Counties'+os.sep+'SumSpecies'+os.sep+csv)
#     [raw_df.drop(m, axis=1, inplace=True) for m in raw_df.columns.values.tolist() if m.startswith('Unnamed')]
#     raw_df = raw_df.reindex(columns = cnty_reindex_cols)
#     ent_id = raw_df['EntityID'].values.tolist()
#     se_entid = pd.Series(ent_id)
#     raw_df.drop('EntityID', axis=1, inplace=True)
#     print raw_df
#     usage_df = apply_usage(raw_df,se_cnty)
#     print usage_df
#     usage_df.insert(loc = 0, column ='EntityID', value=se_entid)
