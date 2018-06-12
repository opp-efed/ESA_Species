import pandas as pd
import os
import datetime

# outputs from splitHUC2_A_B script
in_location = r'D:\Update_AB_splits\SpatialJoins\tables_HUC2'
# Location where final tables will be saved
out_location = r'D:\Update_AB_splits\SpatialJoins\Summarized_spatialJoins'

# the a/b assignment a is above value b is below
#  the a/b assignment is flipped for huc 19, a below 20.58 and b is above see email from Chuck 10/20/16- JC
# Values received from Chuck as the a/b break; see email 10/20/16; based off mean annual precip in inches- JC
# added HI (20) on 6/11/2018 see email from Chuck
split_dict = {'10': 19.75,
              '11': 30.26,
              '12': 29.07,
              '15': 15.25,
              '16': 12.24,
              '17': 30.77,
              '18': 13.74,
              '19': 20.58,
              '20': 75.94
              }

def createdirectory(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        print "created directory {0}".format(new_dir)



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# get a list of all input files
list_csv = os.listdir(in_location)
list_csv = [i for i in list_csv if i.endswith('csv')]
csv = list_csv[0]
# create output folder
createdirectory(out_location)
all_hucs = pd.DataFrame()

# input table and name of output table
in_csv = in_location + os.sep + list_csv[0]  # with seamless data from NHDplus only one csv
out_csv = out_location + os.sep + csv

# read in input and drop index column
in_df = pd.read_csv(in_csv, dtype=object)
in_df  = in_df .loc[in_df ['HUC_12'] != 'None']
in_df.drop(labels=['Unnamed: 0'], axis=1, inplace=True)

# assigns float data type to number and str to HUC12; csv assume data types
in_df['Annual_Prc'] = in_df['Annual_Prc'].map(lambda x: x).astype(float)
in_df['WeatherID'] = in_df['WeatherID'].map(lambda x: x).astype(float)
in_df['HUC_12'] = in_df['HUC_12'].map(lambda x: str(x) if len(str(x)) == 12 else '0' + str(x)).astype(str)

# Extract columns to be used for summary stats
total_precip = in_df[['HUC_12', 'Annual_Prc','WeatherID']]
# sets output files name for intermediate file
total_outcsv = r'D:\Update_AB_splits\SpatialJoins\total_precip'+ os.sep + csv

# removed duplicate values
# keep WeatherID until after remove duplicates so that only true duplicates are removed; ie two point in a HUC12
# may have the annual average  precip and those should NOT be remved
total_precip = total_precip.drop_duplicates()

# saves intermediate file
total_precip.to_csv(total_outcsv)
#total_precip.drop(labels=['WeatherID'], axis=1, inplace=True)  excluded from table in previous step
total_precip['HUC_2'] = total_precip['HUC_12'].map(lambda x: x[:2]).astype(str)

# Loop through each huc2 with split and assigned the a or b based on the info in split_dict
# Generate summary stats (mean, max, min) for each input file;assign a/b based on values in split dict; then save output
for huc2 in split_dict.keys():
    split = split_dict[huc2]

    c_huc2 = total_precip.loc[(total_precip['HUC_2'] == huc2)]
    c_huc2 = c_huc2.ix[:,['HUC_12','Annual_Prc']]
    # generate summary stats as series in a groupby
    att_max = c_huc2.groupby(by=['HUC_12']).max()
    att_min = c_huc2.groupby(by=['HUC_12']).min()
    att_mean = c_huc2.groupby(by=['HUC_12']).mean()
    att_count = c_huc2.groupby(by=['HUC_12']).count()

    # export each summary to a data frame sorted by HUC12 values (str)
    # TODO could export directly to final df and elimate merge at the end
    count_df = pd.DataFrame(att_count)
    count_df.reset_index(level=0, inplace=True)
    count_df = count_df.drop_duplicates()

    count_df.sort_values(['HUC_12'])
    count_df.columns = ['HUC_12', 'Count']

    max_df = pd.DataFrame(att_max)
    max_df.reset_index(level=0, inplace=True)
    max_df = max_df.drop_duplicates()

    max_df.sort_values(['HUC_12'])
    max_df.columns = ['HUC_12', 'Max']

    min_df = pd.DataFrame(att_min)
    min_df.reset_index(level=0, inplace=True)
    min_df = min_df.drop_duplicates()

    min_df.sort_values(['HUC_12'])
    min_df.columns = ['HUC_12', 'Min']

    mean_df = pd.DataFrame(att_mean)
    mean_df.reset_index(level=0, inplace=True)
    mmean_df = mean_df.drop_duplicates()

    mean_df.sort_values(['HUC_12'])
    mean_df.columns = ['HUC_12', 'Mean']
    mean_df['Mean'] = mean_df['Mean'].map(lambda x: "{0:.2f}".format(x))

    # see note at dict to explain what these are different
    # Checks the mean value for each HUC12 and assigned a or b based on values in split_dict
    if huc2 == '19':
        mean_df['HUC_Split'] = mean_df['Mean'].map(lambda x: 'b' if float(x) >= split else 'a')
    else:
        mean_df['HUC_Split'] = mean_df['Mean'].map(lambda x: 'a' if float(x) > split else 'b')

    # Merges the summary stats dfs into a final data frame and exports to final location
    final_df = pd.merge(count_df, max_df, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, min_df, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, mean_df, on='HUC_12', how='outer')
    final_df.to_csv(out_csv)
    all_hucs = pd.concat([all_hucs,final_df], axis=0)

all_hucs['HUC_2'] = all_hucs['HUC_12'].map(lambda x: x[:2]).astype(str)
all_hucs.to_csv(r'D:\Update_AB_splits\SpatialJoins\AllHUC_a_b_' + date+'.csv')
