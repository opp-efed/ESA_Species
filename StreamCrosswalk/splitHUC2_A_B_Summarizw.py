import pandas as pd
import os
import datetime

in_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\tables_HUC2'
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\Summarized_spatialJoins'


def createdirectory(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        print "created directory {0}".format(new_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_csv = os.listdir(in_location)
list_csv = [i for i in list_csv if i.endswith('csv')]

createdirectory(out_location)
for csv in list_csv:
    print csv
    in_csv = in_location + os.sep + csv
    out_csv = out_location + os.sep + csv

    in_df = pd.read_csv(in_csv, dtype=object)
    in_df.drop(labels=['Unnamed: 0'], axis=1, inplace=True)

    in_df['Annual_Prc_x'] = in_df[('Annual_Prc_x')].map(lambda x: x).astype(float)
    in_df['Annual_Prc_y'] = in_df[('Annual_Prc_y')].map(lambda x: x).astype(float)
    in_df['WeatherID_x'] = in_df[('WeatherID_x')].map(lambda x: x).astype(float)
    in_df['WeatherID_y'] = in_df[('WeatherID_y')].map(lambda x: x).astype(float)

    in_df['TotalPrecip'] = in_df[['Annual_Prc_x', 'Annual_Prc_y']].sum(axis=1)
    in_df['WeatherID'] = in_df[['WeatherID_x', 'WeatherID_y']].sum(axis=1)

    total_precip = in_df[['HUC_12', 'TotalPrecip','WeatherID']]
    print total_precip.columns.values.tolist()
    total_outcsv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\total_precip' + os.sep + csv
    print len(total_precip)
    total_precip = total_precip.drop_duplicates()
    print len(total_precip)
    total_precip.to_csv(total_outcsv)
    total_precip.drop(labels=['WeatherID'], axis=1, inplace=True)

    # att_sum = total_precip .groupby(by=['HUC_12']).sum()

    att_max = total_precip.groupby(by=['HUC_12']).max()
    att_min = total_precip.groupby(by=['HUC_12']).min()
    att_mean = total_precip.groupby(by=['HUC_12']).mean()
    att_count = total_precip.groupby(by=['HUC_12']).count()

    # sum_df = pd.DataFrame(att_sum,columns=['TotalPrecip'])
    # sum_df.reset_index(level=0, inplace=True)

    max_df = pd.DataFrame(att_max)
    max_df.reset_index(level=0, inplace=True)
    max_df['HUC_12'] = max_df[('HUC_12')].map(lambda x: x).astype(str)
    max_df.sort_values(['HUC_12'])
    max_df.columns = ['HUC_12', 'Max']

    min_df = pd.DataFrame(att_min)
    min_df.reset_index(level=0, inplace=True)
    min_df['HUC_12'] = min_df[('HUC_12')].map(lambda x: x).astype(str)
    min_df.sort_values(['HUC_12'])
    min_df.columns = ['HUC_12', 'Min']

    mean_df = pd.DataFrame(att_mean)
    mean_df.reset_index(level=0, inplace=True)
    mean_df['HUC_12'] = mean_df[('HUC_12')].map(lambda x: x).astype(str)
    mean_df.sort_values(['HUC_12'])
    mean_df.columns = ['HUC_12', 'Mean']

    count_df = pd.DataFrame(att_count)
    count_df.reset_index(level=0, inplace=True)
    count_df['HUC_12'] = count_df[('HUC_12')].map(lambda x: x).astype(str)
    count_df.sort_values(['HUC_12'])
    count_df.columns = ['HUC_12', 'Count']

    final_df = pd.merge(max_df, min_df, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, mean_df, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, count_df, on='HUC_12', how='outer')
    final_df.to_csv(out_csv)
else:
    pass
