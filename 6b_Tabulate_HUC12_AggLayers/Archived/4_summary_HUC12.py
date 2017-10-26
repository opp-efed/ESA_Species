import pandas as pd
import os
import datetime
import arcpy

#TODO does chuck need the huc12 without use remoced, when merge back to the FC those rows are empty and then filed with 0
# This script will generate the HUC12 tables for the MAGTOOL pulling together
# TODO make a similar script for the BE layers that will pull together the
in_folder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\FinalTables\Merged'
out_folder =r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\FinalTables\PercentOverlap'
# in_folder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\FinalTables\Merged\AA and Aggregates'
# out_folder =r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\FinalTables\Collapased'


# files with added acres col in albers equal area conical
# note relying on the sort feature in pandas to sort the shp and table the same to make sure the correct acres number
# is being linked to the correct HUC12 when there are multiple entries for a single HUC12
in_HUC_base = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\NHD_acresHUC2\acres_sum\outfc'


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


def calculation( in_sum_df, cell_size):

    msq_conversion = cell_size * cell_size
    leading_col = in_sum_df[['HUC12', 'Acres_prj']]
    range_acres_float = in_sum_df[('Acres_prj')].map(lambda x: x)
    list_acres = range_acres_float.values.tolist()
    se = pd.Series(list_acres)

    msq_overlap = in_sum_df[in_sum_df.select_dtypes(include=['number']).columns].multiply(msq_conversion)

    acres_overlap = msq_overlap.multiply(0.000247)
    acres_overlap[('Acres')] = se.values
    percent_overlap = (acres_overlap.div(acres_overlap.Acres, axis='index')) * 100
    percent_overlap.drop(labels=['Acres_prj'], axis=1, inplace=True)
    percent_overlap.drop(labels=['Acres'], axis=1, inplace=True)
    percent_overlap = pd.concat([leading_col, percent_overlap], axis=1)

    return percent_overlap, acres_overlap, msq_overlap

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = in_HUC_base
fc_list = arcpy.ListFeatureClasses()
all_acres = pd.DataFrame(columns=['HUC12', 'Acres_prj'])
for fc in fc_list:
    in_HUC12_shp = in_HUC_base + os.sep + fc
    att_array = arcpy.da.TableToNumPyArray(in_HUC12_shp, ['HUC_12', 'Acres_prj'])
    att_df = pd.DataFrame(data=att_array)
    att_df['HUC_12'] = att_df['HUC_12'].astype(str)
    att_df['HUC_12'] = att_df['HUC_12'].map(lambda x: '0'+x if len(x) == 11 else x).astype(str)
    # att_sum = att_df.groupby(by=['HUC_12'])['Acres_prj'].sum()
    att_sum = att_df.groupby(by=['HUC_12']).sum()
    # att_df = att_sum.to_frame()
    att_sum.reset_index(level=0, inplace=True)
    att_sum['HUC12'] = att_sum['HUC_12'].astype(str)
    att_sum.drop(labels=['HUC_12'], axis=1, inplace=True)
    att_sum.sort_values(['HUC12'])
    all_acres = pd.concat([all_acres, att_sum], axis=0)

all_acres = all_acres.groupby(by=['HUC12']).sum()
all_acres.reset_index(level=0, inplace=True)

temp_folder = out_folder + os.sep + 'Temp'
createdirectory(temp_folder)
all_acres.to_csv(temp_folder + os.sep + 'All_Acres.csv')

list_csv = os.listdir(in_folder)
list_csv = [csv for csv in list_csv if csv.endswith('csv')]

for csv in list_csv:
    print csv
    final_csv = out_folder + os.sep + csv
    final_msq_csv = temp_folder + os.sep + "MSQ_" + csv
    final_in_csv = temp_folder + os.sep + "INdf_" + csv

    in_csv = in_folder + os.sep + csv
    in_df = pd.read_csv(in_csv)
    un_named_col = in_df.columns.values.tolist()
    un_named_col = [col for col in un_named_col if col.startswith('Un')]
    for col in un_named_col:
        in_df.drop(labels=col, axis=1, inplace=True)
    in_df = in_df.fillna(0)
    in_df['HUC12'] = in_df['HUC12'].map(lambda x: x.replace('HUC_1_', '')).astype(str)
    #huc12 = in_df['HUC12'].values.tolist()

    in_df = pd.merge(in_df, all_acres, on='HUC12', how='outer')
    acres_col = in_df['Acres_prj'].map(lambda x: x).astype(float)
    in_df['Acres_prj'] = in_df['Acres_prj'].map(lambda x: x).astype(float)

    percent_overlap, acres_overlap, msq_overlap = calculation(in_df, 30)
    percent_overlap.to_csv(final_csv)
    msq_overlap.to_csv(final_msq_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)


# in_sum = in_df.groupby(by=['HUC12','Acres_prj']).sum()
# in_sum.reset_index(inplace=True)
#
#         in_df = in_sum
#
