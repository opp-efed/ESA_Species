import pandas as pd
import os
import datetime

# Title - Calculate percent overlap based on use and interval for reach species using master sum table and acres table

# User defined variables:

# date in YYYYMMDD
date = 20160808
# Master use list by region and cell size
Co_update = 'J:\Workspace\ESA_Species\FinalBE_ForCoOccur\Dicts\CoUpdate.csv'
# TODO extract this from he master use table and add in what to do with the feature layers to the functions
# Use layer type Raster or Feature
type_use = 'Raster'

# Master table of species that sums pixels by use and distance interval from previous script
in_raw_sum_overlap = 'C:\Workspace\ESA_Species\FinalBE_ForCoOccur\Results_Clipped\Range\outputs' \
                     '\SpeciesGroupsTranspose2\PracticeOverlap_all.csv'
sp_col_count = 4  # number of cols with species info  base 0 found in the sum overlap table

# Master acres for all species by region
in_acres_table = 'J:\Workspace\MasterOverlap\Acres\Range_Acres_20151207.csv'

# Location where output and temp files will be saved
out_location = 'C:\Workspace\ESA_Species\FinalBE_ForCoOccur\Results_Clipped\Range\outputs\SpeciesGroupsTranspose2'

# List of regions that are completed and should be included
regions = ['CONUS']

# Dictionary that provides a list of cell size found in the suite of uses by region
use_cell_size = {'AK': [30],
                 'AS': [30],
                 'CNMI': [30],
                 'CONUS': [30],
                 'GU': [2.4, 30],
                 'HI': [30],
                 'PR': [30],
                 'VI': [2.4, 30]
                 }


# TODO ADD IN VECTOR OVERLAP
def calculation(typefc, range_acres, in_sum_df, cell_size):
    if typefc == "Raster":
        msq_conversion = cell_size * cell_size
        msq_overlap = in_sum_df[in_sum_df.select_dtypes(include=['number']).columns].multiply(msq_conversion)
        acres_overlap = msq_overlap.multiply(0.000247)
        percent_overlap = (acres_overlap.div(range_acres.ix[0], axis='columns')) * 100
        return percent_overlap
    else:
        print "ERROR ERROR"


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
# 1) Read in master tables

# read in master use list with cell size info for each use
co_update_lookup = pd.read_csv(Co_update)

# read in master raw sum table; apply data types; split spe info and use info into two dataframes
sum_df = pd.read_csv(in_raw_sum_overlap, dtype=object)
sum_df['EntityID'] = sum_df['EntityID'].astype(str)
sum_df.sort_values(['EntityID'])
header_sp = sum_df.columns.values.tolist()
# Species info only df, used as backbone of the final_df so that evert species from master is accounted for
# As the script loops through the regions and uses  dfs
species_info_df = sum_df.iloc[:, :sp_col_count]

sum_df = sum_df.iloc[:, (sp_col_count + 1):(len(header_sp))].apply(pd.to_numeric)  # convert all use values to numeric
sum_df = pd.concat([species_info_df, sum_df], axis=1)  # concat back to sp df
range_unaccounted_for = sum_df.loc[sum_df['CONUS_Corn_0'] < 0]  # Ranges not run error code -88888
# TODO Take out this hard wire to column name

# read master acres for all species by region
acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].astype(str)
acres_df.sort_values(['EntityID'])

# 2) Filter master table so that all sorted by EntityID and only contain those EntityID that are found all both the sum
# and the acres tables

# Extracts species that are complete on the sum table but not found on acres table
#  species will be exported to their own table at end to be added to acres
missing_from_acres = sum_df[sum_df['EntityID'].isin(acres_df['EntityID']) == False]

# filters sum table so only includes EntID in common with acres table
sum_df_found_in_acres = sum_df[sum_df['EntityID'].isin(acres_df['EntityID']) == True]

# 3) Final Data frames that are read into the loop to cal percent overlap by region and uses by cell size

# Filters out species that have been flagged runs have not started from above df( ie error code -88888)
# this is the raw sum df that will be read into the loop to cal per overlap by region and uses by cell size

sum_df_found_in_acres = sum_df_found_in_acres[
    sum_df_found_in_acres['EntityID'].isin(range_unaccounted_for['EntityID']) == False]
sum_df_found_in_acres.sort_values(['EntityID'])
# Filters acres table so that  only EntID in common with above sum_df_in_acres data frame(removes sp not started -88888)
acres_in_sum = acres_df[acres_df['EntityID'].isin(sum_df_found_in_acres['EntityID']) == True]
acres_in_sum.sort_values(['EntityID'])

# filters sp info df so that only EntID in common with above sum_df_in_acres data frame(removes sp not started -88888)
# this table will be used as backbone for out table- each percent overlap loop is merged to this table column wise
# NOTE NOTE if values are not matching to the correct species it is probably do to a failed sort

# NOTE NOTE all table should be filter to common EntityID and then sorted based on EntityID so values should match
# NOTE NOTE if this becomes a problem try keeping in the EntityID column for the percent overlap and them use
# final_df= pd.merge(final_df, percent_overlap_df, on='EntityID', how='left') to merge rather than concat column wise

filter_species_info = species_info_df[species_info_df['EntityID'].isin(sum_df_found_in_acres['EntityID']) == True]

# filter_species_info used back bone of the final data frame,
# data frame with all of the species and species info common to both tables (removes sp not started -88888)
final_df = filter_species_info

# 4) Run percent overlap by region, by use and cell size
for region in regions:
    cnt_cha = len(region)
    # Extracts all cols from acres table for current region and sets the values to float
    # NOTE NOTE error will occur if value in acres table is NAN or not numeric
    acres_col = acres_in_sum[('Acres_' + str(region))].map(lambda x: x).astype(float)
    cells_list = use_cell_size[region]
    # list of all uses for the current region from master use table
    filter_regions = co_update_lookup.loc[co_update_lookup['Region'] == region]
    for cell in cells_list:
        region_csv = out_location + os.sep + str(region) + '_' + str(cell) + '_' + str(date) + '.csv'
        print 'Working on Region {0} with use cell size {1}\nTemp file will be saved at {2}'.format(region, cell,
                                                                                                    region_csv)
        # List of uses for current region with the current cell size
        filter_use_cell = filter_regions[filter_regions['cell'] == cell]
        use_starts_with_list = filter_use_cell['Use'].values.tolist()
        # list of all col header from sum table for current region ie CONUS_Corn_0 ..
        region_uses = [word for word in header_sp if word[:cnt_cha] == region]
        cell_region_uses = []

        # compares use list for region from master use, to col header from the sum table to get a list of col header to
        # be included in this loop

        for v in region_uses:
            break_value = v.split('_')
            check_value = break_value[0] + '_' + break_value[1]
            if check_value in use_starts_with_list:
                cell_region_uses.append(v)
            else:
                pass
        # appends only the use columns from use for that region with that cell size

        # sub_sets sum df to only those columns appened to cell_region_uses in the above loop
        region_use_df = sum_df_found_in_acres[cell_region_uses]

        # Runs percent overlap calculation
        percent_overlap_df = calculation(type_use, acres_col, region_use_df, cell)
        # Neg values indicated a species that are partially complete, or error code -55555 from the sum table
        # all neg values are updated to the -55555 errror code

        num = percent_overlap_df._get_numeric_data()
        num[num < 0] = -55555

        # Appends the current percent overlap_df to the final_df across columns
        # See Note above if rows are not lining up correctly

        final_df = pd.concat([final_df, percent_overlap_df], axis=1)
        final_df.to_csv(region_csv)


# add the species that have not been start back to the final df ie error code -88888
final_df_neg_88888 = pd.concat([final_df, range_unaccounted_for])

# cross checks the list of species not found acres table to the species not run yet (-88888)
# Will only export species that have been run but are missing from the acres table
missing_acres_table = missing_from_acres[missing_from_acres['EntityID'].isin(final_df_neg_88888['EntityID']) == False]

# 5)  Export Table

out_csv = out_location + os.sep + 'Percent_Overlap_all_Intervals_' + str(date) + '.csv'
final_df_neg_88888.to_csv(out_csv)

need_add_acres_table = out_location + os.sep + 'add_to_acres_' + str(date) + '.csv'
missing_acres_table.to_csv(need_add_acres_table)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
