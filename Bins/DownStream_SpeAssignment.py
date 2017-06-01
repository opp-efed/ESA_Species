import pandas as pd
import datetime
in_wide_table ='C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\UsedFinalBE_20170110.csv'
out_csv ='C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\UsedFinalBE_20170110_DDspecies.csv'

def flag_dd_species (row):
    # VARS: row: row of data being updated in working df
    # DESCRIPTION: Flags species, Yes/No, as a DD based on bin assignment
    # Try/Except catches any species still without a bin assignment and does not flag it as a DD species
    # Logic for assignment: If a species is assigned or reassigned as a surrogate to Bins 2,3, or 4 they are in the
    # analysis. If the species is a mammal or bird and has a food item in Bin 2,3, or 4, they are also in the analysis.
    # If the species is a mammal or bird and has food items only found in marine bin that were reassigned to Bin 2,3 or
    # 4, they are not in the analysis. If a species is only assigned to Bins 5, 6, or 7, they are not in the analysis.
    # If the species is a mammal or bird and has a food item only in 5, 6, or 7,they are also not in the analysis.
    # RETURN: updated values for column being updated by apply function
    if row['Bin 2'].startswith('Yes'):
        return 'Yes'
    elif row['Bin 3'].startswith('Yes'):
        return 'Yes'
    elif row['Bin 4'].startswith('Yes'):
        return 'Yes'
    elif row['Bin 2'].startswith('Food item'):
        return 'Yes'
    elif row['Bin 3'].startswith('Food item'):
        return 'Yes'
    elif row['Bin 4'].startswith('Food item'):
        return 'Yes'
    else:
        return 'No'



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

table_df = pd.read_csv(in_wide_table, dtype=object)
table_df ['DD_Species'] = table_df.apply(lambda row: flag_dd_species(row), axis=1)

table_df.to_csv(out_csv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
