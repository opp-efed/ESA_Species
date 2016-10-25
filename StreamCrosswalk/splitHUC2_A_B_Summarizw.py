import pandas as pd
import os
import datetime

# outputs from splitHUC2_A_B script
in_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\tables_HUC2'
# Location where final tables will be saved
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\Summarized_spatialJoins'

# the a/b assigment is fliped for huc 19, a below 20.58 and b is above see email from Chuck 10/20/16- JC
# Values received from Chuck as the a/b break; see email 10/20/16; based off mean annual precip in inches- JC

split_dict = {'10L': 19.75,
              '10U': 19.75,
              '11': 30.26,
              '12': 29.07,
              '15': 15.25,
              '16': 12.24,
              '17': 30.77,
              '18': 13.74,
              '19': 20.58,
              }

def createdirectory(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        print "created directory {0}".format(new_dir)



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
# get a list of all input files
list_csv = os.listdir(in_location)
list_csv = [i for i in list_csv if i.endswith('csv')]
# create output folder
createdirectory(out_location)

# Generate summary stats (mean, max, min) for each input file;assign a/b based on values in split dict; then save output
for csv in list_csv:
    print csv
    # Current HUC2 values, extracted from input csv name; used to extract split value from split_dict
    huc = str(csv.split("_")[1])
    huc = huc.replace('.csv', '')
    split = split_dict[huc]

    # input table and name of output table
    in_csv = in_location + os.sep + csv
    out_csv = out_location + os.sep + csv

    # read in input and drop index column
    in_df = pd.read_csv(in_csv, dtype=object)
    in_df.drop(labels=['Unnamed: 0'], axis=1, inplace=True)

    # assigns float data type; csv assume data types
    in_df['Annual_Prc_x'] = in_df['Annual_Prc_x'].map(lambda x: x).astype(float)
    in_df['Annual_Prc_y'] = in_df['Annual_Prc_y'].map(lambda x: x).astype(float)
    in_df['WeatherID_x'] = in_df['WeatherID_x'].map(lambda x: x).astype(float)
    in_df['WeatherID_y'] = in_df['WeatherID_y'].map(lambda x: x).astype(float)

    # Merges results from the two spatial joins together; only one set of columns is completed x or y so summing then
    # gives the results of all HUCs in a single field
    in_df['Annual_Prc'] = in_df[['Annual_Prc_x', 'Annual_Prc_y']].sum(axis=1)
    in_df['WeatherID'] = in_df[['WeatherID_x', 'WeatherID_y']].sum(axis=1)

    # Extract columns to be used for summary stats
    total_precip = in_df[['HUC_12', 'Annual_Prc', 'WeatherID']]
    print total_precip.columns.values.tolist()
    # sets output files name for intermediate file
    total_outcsv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\total_precip' \
                   + os.sep + csv
    print len(total_precip)
    # removed duplicate values see TODO in splitHUC2_A_B;
    # keep WeatherID until after remove duplicates so that only true duplicates are removed; ie two point in a HUC12
    # may have theannual average  precip and those should NOT be remved

    total_precip = total_precip.drop_duplicates()
    print len(total_precip)
    # saves intermediate file
    total_precip.to_csv(total_outcsv)
    total_precip.drop(labels=['WeatherID'], axis=1, inplace=True)

    # generate summary stats as series in a groupby
    att_max = total_precip.groupby(by=['HUC_12']).max()
    att_min = total_precip.groupby(by=['HUC_12']).min()
    att_mean = total_precip.groupby(by=['HUC_12']).mean()
    att_count = total_precip.groupby(by=['HUC_12']).count()

    # export each summary to a data frame sorted by HUC12 values (str)
    # TODO to speed this up; export direcly to final df and elimate merge at the end
    # TODO set up function get dynamically generate a suite of summary stats and split based on user inputed list;
    count_df = pd.DataFrame(att_count)
    count_df.reset_index(level=0, inplace=True)
    count_df['HUC_12'] = count_df['HUC_12'].map(lambda x: x).astype(str)
    count_df.sort_values(['HUC_12'])
    count_df.columns = ['HUC_12', 'Count']

    max_df = pd.DataFrame(att_max)
    max_df.reset_index(level=0, inplace=True)
    max_df['HUC_12'] = max_df['HUC_12'].map(lambda x: x).astype(str)
    max_df.sort_values(['HUC_12'])
    max_df.columns = ['HUC_12', 'Max']

    min_df = pd.DataFrame(att_min)
    min_df.reset_index(level=0, inplace=True)
    min_df['HUC_12'] = min_df['HUC_12'].map(lambda x: x).astype(str)
    min_df.sort_values(['HUC_12'])
    min_df.columns = ['HUC_12', 'Min']

    mean_df = pd.DataFrame(att_mean)
    mean_df.reset_index(level=0, inplace=True)
    mean_df['HUC_12'] = mean_df['HUC_12'].map(lambda x: x).astype(str)
    mean_df.sort_values(['HUC_12'])
    mean_df.columns = ['HUC_12', 'Mean']
    mean_df['Mean'] = mean_df['Mean'].map(lambda x: "{0:.2f}".format(x))

    # see note at dict to explain what these are different
    # Checks the mean value for each HUC12 and assigned a or b based on values in split_dict
    if huc == '19':
        mean_df['HUC_Split'] = mean_df['Mean'].map(lambda x: 'b' if float(x) > split else 'a')
    else:
        mean_df['HUC_Split'] = mean_df['Mean'].map(lambda x: 'a' if float(x) > split else 'b')

    # Merges the summary stats dfs into a final data frame and exports to final location
    final_df = pd.merge(count_df, max_df, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, min_df, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, mean_df, on='HUC_12', how='outer')
    final_df.to_csv(out_csv)

