import pandas as pd

import datetime

# Title- convert acres table to a different area unit

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# User defined variables:
# date in YYYYMMDD
date = 20190812

# Conversion value
conversion = 0.0015625
# takes conversion number and puts it into scientific notation for file name
sci_notation = str("{:.4E}".format(conversion))


# Master table of acres
in_table = 'L:\Workspace\StreamLine\ESA\CompositeFiles_Summer2019\CH_Acres_by_region_20190812.csv'
out_csv = 'L:\Workspace\StreamLine\ESA\CompositeFiles_Summer2019\CH_ConvertedAcres_SqMiles_' + str(
    sci_notation) + '_byregion' + str(date) + '.csv'
# number of cols with species info  base 0 found in the Acres table, these col do not need to be converted
sp_col_count = 20


def calculation(in_df, conversion_val):
    conversion_df = in_df[in_df.select_dtypes(include=['number']).columns].multiply(conversion_val)
    return conversion_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


# 1) Read in master tables
# read in master raw sum table; apply data types; split spe info and use info into two dataframes
area_df = pd.read_csv(in_table, dtype=object)
area_df['EntityID'] = area_df['EntityID'].astype(str)
area_df.sort_values(['EntityID'])
header_sp = area_df.columns.values.tolist()

# Species info only df, used as backbone of the final_df so that every species from master is accounted for
species_info_df = area_df.iloc[:, :sp_col_count]
# extracts the col with the area values
area_df = area_df.iloc[:, sp_col_count:(len(header_sp))].apply(pd.to_numeric)  # convert all area a values to numeric


# 2) Runs conversion
converted_df = calculation(area_df, conversion)
print converted_df
final_df = pd.concat([species_info_df, converted_df], axis=1)  # concat back to sp df
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
