import datetime
import os
import arcpy

import pandas as pd

# Title- Generate overlap tables from zone species rasters to use layers results;
#               1) Generates tables for aggregated layers, AA, Ag and NonAG
#                       1a) The final merged output are used to generate distance interval table for spray drift; and
#                           summarized BE table (0, 305m and 765)
# TODO Add look to read in vector table once vector overlap final

# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS
# Species group is found in index position 1 of all input result tables when split by '_'
# All raster are 30 meter cells - note previously VI and CNMI has some use with a different cell size

# ###############user input variables
#overwrite boolean - set to false tables were already generated for some uses and new ones need to be added.  If a use
# layer was updated delete or archive the tables for the dated version and set this to false.  If this variable is set
# to TRUE than all tables will be recalculated.
overwrite_inter_data = True
# file structure is standard for raw result outputs and tabulated results outputs
# Changes include L48 v NL48  and Range and CriticalHabitat in the path
raw_results_csv = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Tabulated_byState'
out_location =  r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Overlap_byState'
out_location_merge =  r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Overlap_byState_Merge'
in_acres_table = r'L:\Workspace\StreamLine\ESA\R_Acres_Pixels_20180428.csv'
look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSupportingTables\Uses_lookup_20180430.csv'
id_value = 'Dicamba'
master_list = r'C:\Users\JConno02' \
              r'\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].astype(str)
use_lookup = pd.read_csv(look_up_use)

# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


def calculation(type_fc, in_sum_df, c_region, percent_type):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.select_dtypes(include=['number']).columns.values.tolist()
    if percent_type == 'full range':
        acres_col = 'TotalAcresOnLand'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'NL48 range':
        acres_col = 'TotalAcresNL48'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'regional range':
        acres_col = 'Acres_' + str(c_region)
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]


    if type_fc == "Raster":
        overlap = in_sum_df.copy()
        # convert msq to acres
        overlap.ix[:, use_cols] *= 0.000247

        # generate percent overlap by taking acres of use divided by total acres of the species range
        overlap[use_cols] = overlap[use_cols].div(overlap[acres_col], axis=0)
        overlap.ix[:, use_cols] *= 100

        # Drop excess acres col- both regional and full range are included on input df; user defined parameter
        # percent_type determines which one is used in overlap calculation
        if percent_type == 'full range':
            overlap.drop('TotalAcresNL48', axis=1, inplace=True)
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)

        elif percent_type == 'NL48 range':
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)

        elif percent_type == 'regional range':
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)
            overlap.drop('TotalAcresNL48', axis=1, inplace=True)
        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print "ERROR ERROR"


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_location)

list_csv = os.listdir(raw_results_csv)
list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

final_df = base_sp_df.copy()
out_csv = out_location_merge + os.sep + 'Merge_' + id_value + "_"+date+'.csv'
for csv in list_csv:
    use_nm = csv.replace('_euc.csv','')
    region = csv.split("_")[0]
    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
    # load outside function due to double _ added by filename by default w. arcpy- corrected moving forward
    df_use = pd.read_csv(raw_results_csv + os.sep  + csv, dtype=object)
    type_use = use_lookup.loc[use_lookup['FullName'] == use_nm, 'Type'].iloc[0]
    final_col_header = use_lookup.loc[use_lookup['FullName'] == use_nm, 'FinalColHeader'].iloc[0]
    print use_nm, final_col_header
    out_use_overlap = out_location + os.sep + csv
    use_array = pd.read_csv(raw_results_csv + os.sep + csv)
    use_array ['EntityID'] = use_array ['EntityID'].map(lambda x: x).astype(str)

    # STEP 2: Run percent overlap for both the full range and the regional; set up acres table to include only
    # species found on the current use table (use_array)

    sum_df_acres = pd.merge(use_array, acres_for_calc, on='EntityID', how='left')
    print 'Generating regional range percent overlap...:{0}'.format(csv)
    percent_overlap_df_region = calculation(type_use, sum_df_acres, region, 'regional range')
    # print list(set(percent_overlap_df_region ['EntityID'].values.tolist()))
    [percent_overlap_df_region.drop(m, axis=1, inplace=True) for m in
    percent_overlap_df_region.columns.values.tolist() if m.startswith('Unnamed')]
    percent_overlap_df_region.to_csv(out_use_overlap)

    # STEP 3: Merge Tables by use for the full range and regional range
    create_directory(out_location_merge)
    print'Merging overlap table regional range...{0}'.format(use_nm)
    list_percent_csv = os.listdir(out_location)
    list_percent_csv = [csv for csv in list_percent_csv if csv.endswith('.csv')]
    for csv in list_percent_csv:
        current_csv = out_location + os.sep + csv
        in_df = pd.read_csv(current_csv, dtype=object)
        out_df = pd.merge(base_sp_df, in_df, on='EntityID', how='left')
        [out_df .drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist() if m.startswith('Unnamed')]
        out_df.fillna(0, inplace=True)
        out_df.to_csv (out_location_merge + os.sep +  csv)
#         final_df = pd.merge(final_df, in_df, on='EntityID', how='left')
#
# final_df.to_csv(out_csv)


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
