import pandas as pd
import os
import datetime

# Title - Calculate percent overlap based on use and interval for reach species using master sum table and acres table

# User defined variables:

# date in YYYYMMDD
date = 20170103
# Master use list by region and cell size

# TODO extract this from he master use table and add in what to do with the feature layers to the functions
# Use layer type Raster or Feature
type_use = 'Raster'

# Master table of species that sums pixels by use and distance interval from previous script
#in_raw_sum_overlap = r'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat\SumSpecies'
in_raw_sum_overlap = r'E:\Tabulated_NewComps\NL48\CriticalHabitat\SumSpecies'
sp_col_count = 7  # number of cols with species info  base 0 found in the sum overlap table

# Master acres for all species by region
#in_acres_table = r'E:\Tables\CH_Acres_by_region_20161215.csv'
in_acres_table = r'E:\Tables\CH_Acres_by_region_20161215.csv'

# Location where output and temp files will be saved
#out_folder = r'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat\PercentOverlap'
out_folder = r'E:\Tabulated_NewComps\NL48\CriticalHabitat\PercentOverlap'
# out_csv_region = out_location + os.sep + 'Percent_Overlap_all_IntervalsRegion_' + str(date) + '.csv'
# out_csv = out_location + os.sep + 'Percent_Overlap_all_IntervalsFull_' + str(date) + '.csv'

# out_csv_region2 = out_location + os.sep + 'Percent_Overlap_all_IntervalsRegion2_' + str(date) + '.csv'
# out_csv2 = out_location + os.sep + 'Percent_Overlap_all_IntervalsFull2_' + str(date) + '.csv'
# List of regions that are completed and should be included

# regions = ['CONUS']

# Dictionary that provides a list of cell size found in the suite of uses by region, previouls CNMI and VI
use_cell_size = {'AK': [30],
                 'AS': [30],
                 'CNMI': [30],
                 'CONUS': [30],
                 'GU': [30],
                 'HI': [30],
                 'PR': [30],
                 'VI': [30]
                 }

# TODO ADD IN VECTOR OVERLAP
def calculation(typefc, in_sum_df, cell_size, c_region):

    range_acres_float = in_sum_df[('Acres_' + str(c_region))].map(lambda x: x).astype(float).map(lambda x: x).astype(
        float)
    list_acres = range_acres_float.values.tolist()
    se = pd.Series(list_acres)

    if typefc == "Raster":
        msq_conversion = cell_size * cell_size
        msq_overlap = in_sum_df[in_sum_df.select_dtypes(include=['number']).columns].multiply(msq_conversion)

        acres_overlap = msq_overlap.multiply(0.000247)
        acres_overlap[('Acres')] = se.values
        #print acres_overlap

        percent_overlap = (acres_overlap.div(acres_overlap.Acres, axis='index')) * 100
        percent_overlap[('Acres_' + str(region))] = se.values

        return percent_overlap
    else:
        print "ERROR ERROR"


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
createdirectory(out_folder)
# 1) Read in master tables

# read in master use list with cell size info for each use


# read in master raw sum table; apply data types; split spe info and use info into two dataframes

list_folder = os.listdir(in_raw_sum_overlap)

for folder in list_folder:
    current_folder = in_raw_sum_overlap + os.sep + folder
    list_csv = os.listdir(current_folder)
    for csv in list_csv:

        region = csv.replace("__","_")
        region = region.split("_")[2]
        print region
        cells_list = 30

        out_location = out_folder + os.sep + folder
        createdirectory(out_location)
        out_csv = out_location + os.sep + csv
        # CHECK FOR MISSING TABLE WHEN RUN A SECOND TIME
        # if not os.path.exists(out_csv):
            #print '\n new table{0}'.format(out_csv)
        current_csv = in_raw_sum_overlap + os.sep + folder + os.sep + csv
        sum_df = pd.read_csv(current_csv, dtype=object)
        sum_df['EntityID'] = sum_df['EntityID'].astype(str)
        sum_df.sort_values(['EntityID'])
        header_sp = sum_df.columns.values.tolist()
        species_info_df = sum_df.iloc[:, :sp_col_count]

        sum_df = sum_df.iloc[:, (sp_col_count + 1):(len(header_sp))].apply(pd.to_numeric)  # convert to numeric
        sum_df = pd.concat([species_info_df, sum_df], axis=1)  # concat back to sp df

        acres_df = pd.read_csv(in_acres_table)

        acres_df['EntityID'] = acres_df['EntityID'].astype(str)
        acres_df.sort_values(['EntityID'])

        # filters sum table so only includes EntID in common with acres table
        sum_df_found_in_acres = sum_df[sum_df['EntityID'].isin(acres_df['EntityID']) == True]

        sum_df_found_in_acres.sort_values(['EntityID'])
        sum_df.sort_values(['EntityID'])

        acres_in_sum = acres_df[acres_df['EntityID'].isin(sum_df_found_in_acres['EntityID']) == True]
        acres_in_sum.sort_values(['EntityID'])

        filter_species_info = species_info_df[
            species_info_df['EntityID'].isin(sum_df_found_in_acres['EntityID']) == True]

        total_acres = acres_in_sum[('TotalAcresOnLand')].map(lambda x: x).astype(float)


        cnt_cha = len(region)

        acres_in_sum = acres_in_sum.ix[:,['EntityID', ('Acres_' + str(region))]]
        acres_in_sum[('Acres_' + str(region))] = acres_in_sum[('Acres_' + str(region))].map(lambda x: x).fillna(-1)

        sum_df_acres = pd.merge(sum_df, acres_in_sum, on='EntityID', how='left')


        percent_overlap_df_full = calculation(type_use, sum_df_acres, 30, region)
        final_df = pd.concat([species_info_df, percent_overlap_df_full], axis=1)

        final_df.drop('Unnamed: 0', axis=1, inplace=True)

        final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
