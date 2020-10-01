import pandas as pd
import os
import datetime

# Title - Calculate percent overlap based on use and interval for reach species using master sum table and acres table

# User defined variables:


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# Master use list by region and cell size

# TODO extract this from he master use table and add in what to do with the feature layers to the functions

# Use layer type Raster or Feature
type_use = 'Raster'
# Master table of species that sums pixels by use and distance interval from previous script
# L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\FederalLands\NL48\Range\\SumSpecies
in_raw_sum_overlap = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\FederalLands' \
                     r'\NL48\Range\SumSpecies'
sp_col_count = 7  # number of cols with species info  base 0 found in the sum overlap table

# Master acres for all species by region

in_acres_list = [
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\CH_Acres_by_region_20170208.csv',
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20161216.csv']
percent_regional = False

# Location where output and temp files will be saved

out_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\FederalLands' \
             r'\NL48\Range\test'

# List of regions that are completed and should be included

# regions = ['CONUS']

# Dictionary that provides a list of cell size found in the suite of uses by region, previouls CNMI and VI
use_cell_size = {'AK': [30], 'AS': [30], 'CNMI': [30], 'CONUS': [30], 'GU': [30], 'HI': [30], 'PR': [30], 'VI': [30]}


def calculation(typefc, in_sum_df, cell_size, c_region, percent_type):
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

    if typefc == "Raster":
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


def createdirectory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        print "created directory {0}".format(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
createdirectory(out_folder)
# Runs check to determine if the Range or CH acres table should be loaded
path_check_ch_r, sum_folder = os.path.split(in_raw_sum_overlap)
path, ch_r_folder = os.path.split(path_check_ch_r)
if ch_r_folder == 'Range':
    in_acres_table = in_acres_list[1]
else:
    in_acres_table = in_acres_list[0]

list_folder = os.listdir(in_raw_sum_overlap)
for folder in list_folder:
    current_folder = in_raw_sum_overlap + os.sep + folder
    list_csv = os.listdir(current_folder)
    out_location = out_folder + os.sep + folder
    createdirectory(out_location)
    for csv in list_csv:
        current_csv = in_raw_sum_overlap + os.sep + folder + os.sep + csv
        out_csv = out_location + os.sep + csv
        region = csv.replace("__", "_")
        region = region.split("_")[2]
        regional_cell_size = use_cell_size[region][0]

        if not os.path.exists(out_csv):
            print '\n new table{0}'.format(out_csv)

        sum_df = pd.read_csv(current_csv, dtype=object)
        acres_df = pd.read_csv(in_acres_table)
        sum_df['EntityID'] = sum_df['EntityID'].astype(str)
        acres_df['EntityID'] = acres_df['EntityID'].astype(str)
        acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresOnLand']]

        # add the regional and full range acres values to the input df; left merge so only species found on current use
        # table will have acres added
        sum_df_acres = pd.merge(sum_df, acres_for_calc, on='EntityID', how='left')

        percent_overlap_df_full = calculation(type_use, sum_df_acres, regional_cell_size, region, percent_regional)
        [percent_overlap_df_full.drop(m, axis=1, inplace=True) for m in percent_overlap_df_full.columns.values.tolist()
         if m.startswith('Unnamed')]
        percent_overlap_df_full.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
