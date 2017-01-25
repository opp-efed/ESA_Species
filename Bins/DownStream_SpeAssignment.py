import pandas as pd
import datetime
in_wide_table ='C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\UsedFinalBE_20170110.csv'
out_csv ='C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\UsedFinalBE_20170110_DDspecies.csv'

def flag_dd_species (row):
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
