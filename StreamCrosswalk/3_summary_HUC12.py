import pandas as pd
import os
import datetime
import arcpy

inFolder_AG = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\HUC12\Tabulated\YearlyCDL\HUC12_transposed'
outFolder_AG = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_percentOverlap'

inFolder_NonAG = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_transposed_NonAG'
outFolder_NonAG = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_percentOverlap_NonAG'

# files with added acres col in albers equal area conical
# note relying on the sort feature in pandas to sort the shp and table the same to make sure the correct acres number
# is being linked to the correct HUC12 when there are multiple entries for a single HUC12
in_HUC_base = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\outfc'


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


def calculation(range_acres, in_sum_df, cell_size):

    msq_conversion = cell_size * cell_size
    leading_col = in_sum_df[['HUC12', 'Acres_prj']]

    msq_overlap = in_sum_df[in_sum_df.select_dtypes(include=['number']).columns].multiply(msq_conversion)

    acres_overlap = msq_overlap.multiply(0.000247)
    percent_overlap = (acres_overlap.div(range_acres, axis=0)) * 100


    percent_overlap = pd.concat([leading_col, percent_overlap], axis=1)

    print len(percent_overlap)
    return percent_overlap, acres_overlap, msq_overlap


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\outfc'
fc_list = arcpy.ListFeatureClasses()

list_folder = os.listdir(inFolder_AG)
for folder in fc_list:
    in_HUC12_shp = in_HUC_base + os.sep + folder
    folder = folder.replace('_WBD.shp', '')
    print folder


    out_folder = outFolder_AG + os.sep + folder
    att_array = arcpy.da.TableToNumPyArray(in_HUC12_shp, ['HUC_12', 'Acres_prj'])

    att_df = pd.DataFrame(data=att_array)
    att_df['HUC_12'] = att_df['HUC_12'].astype(str)
    att_df['HUC_12'] = att_df['HUC_12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)

    att_sum = att_df.groupby(by=['HUC_12'])['Acres_prj'].sum()
    att_df = att_sum.to_frame()
    att_df.reset_index(level=0, inplace=True)

    att_df['HUC12'] = att_df['HUC_12'].astype(str)
    att_df.drop(labels=['HUC_12'], axis=1, inplace=True)
    att_df.sort_values(['HUC12'])

    csvFolder = inFolder_AG + os.sep + folder
    out_folder = outFolder_AG + os.sep + folder
    temp_folder = out_folder + os.sep + 'Temp'
    createdirectory(out_folder)
    createdirectory(temp_folder)

    HUC2 = folder.replace('NHDPlus', '_')

    list_csv = os.listdir(csvFolder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    list_csv = [csv for csv in list_csv if not csv.startswith('Albers')]
    for csv in list_csv:
        final_csv = out_folder + os.sep + csv
        final_acres_csv = temp_folder + os.sep + 'Acres_' + csv
        final_msq_csv = temp_folder + os.sep + "MSQ_" + csv
        final_in_csv = temp_folder + os.sep + "INdf_" + csv

        if not os.path.exists(final_csv):
            in_csv = csvFolder + os.sep + csv

            in_df = pd.read_csv(in_csv)


            in_df = in_df.ix[1:]
            in_df['HUC12'] = in_df['Unnamed: 0'].map(lambda x: x.replace('HUC_1_', '')).astype(str)
            huc12 = in_df['HUC12'].values.tolist()
            in_df.drop(labels=['Unnamed: 0'], axis=1, inplace=True)
            in_df.drop(labels=['HUC12'], axis=1, inplace=True)
            in_df.insert(0, 'HUC12', huc12)
            in_df['HUC12'] = in_df['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)

            in_df = pd.merge(att_df, in_df, on='HUC12', how='left')

            if len(att_df) != len(in_df):
                print 'Shapes had lengether {0} and df ahs lengert {1}'.format(len(att_df), len(in_df))
                in_df.to_csv(
                    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_percentOverlap\NHDPlus01\{0}.csv'.format(
                        csv))
            in_df.sort_values(['HUC12'])
            in_df['HUC12'] = in_df['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
            in_sum = in_df.groupby(by=['HUC12','Acres_prj']).sum()
            in_sum.reset_index( inplace=True)

            in_df = in_sum
            in_df.to_csv(final_in_csv)

            acres_col = in_df[('Acres_prj')].map(lambda x: x).astype(float)
            in_df['Acres_prj'] = in_df[('Acres_prj')].map(lambda x: x).astype(str)
            percent_overlap, acres_overlap, msq_overlap = calculation(acres_col, in_df, 30)
            percent_overlap.to_csv(final_csv)
            acres_overlap.to_csv(final_acres_csv)
            msq_overlap.to_csv(final_msq_csv)
        else:
            pass
            # print 'Already exported file {0}'.format(final_csv)

list_folder = os.listdir(inFolder_NonAG)

for folder in fc_list:
    in_HUC12_shp = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\outfc' + os.sep + folder
    folder = folder.replace('_WBD.shp', '')
    print folder

    out_folder = outFolder_NonAG + os.sep + folder
    att_array = arcpy.da.TableToNumPyArray(in_HUC12_shp, ['HUC_12', 'Acres_prj'])

    att_df = pd.DataFrame(data=att_array)
    att_df['HUC_12'] = att_df['HUC_12'].astype(str)
    att_df['HUC_12'] = att_df['HUC_12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)

    att_sum = att_df.groupby(by=['HUC_12'])['Acres_prj'].sum()
    att_df = att_sum.to_frame()
    att_df.reset_index(level=0, inplace=True)

    att_df['HUC12'] = att_df['HUC_12'].astype(str)
    att_df.drop(labels=['HUC_12'], axis=1, inplace=True)
    att_df.sort_values(['HUC12'])

    csvFolder = inFolder_NonAG + os.sep + folder
    out_folder = outFolder_NonAG + os.sep + folder
    temp_folder = out_folder + os.sep + 'Temp'
    createdirectory(out_folder)
    createdirectory(temp_folder)

    HUC2 = folder.replace('NHDPlus', '_')

    list_csv = os.listdir(csvFolder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    list_csv = [csv for csv in list_csv if csv.startswith('Albers')]
    for csv in list_csv:
        final_csv = out_folder + os.sep + csv
        print final_csv
        final_acres_csv = temp_folder + os.sep + 'Acres_' + csv
        final_msq_csv = temp_folder + os.sep + "MSQ_" + csv
        final_in_csv = temp_folder + os.sep + "INdf_" + csv

        if not os.path.exists(final_csv):
            in_csv = csvFolder + os.sep + csv

            in_df = pd.read_csv(in_csv)
            in_df = in_df.ix[1:]
            in_df['HUC12'] = in_df['Unnamed: 0'].map(lambda x: x.replace('HUC_1_', '')).astype(str)
            huc12 = in_df['HUC12'].values.tolist()
            in_df.drop(labels=['Unnamed: 0'], axis=1, inplace=True)
            in_df.drop(labels=['HUC12'], axis=1, inplace=True)
            in_df.insert(0, 'HUC12', huc12)
            in_df = pd.merge(att_df, in_df, on='HUC12', how='left')
            if len(att_df) != len(in_df):
                print 'Shapes had lengether {0} and df ahs lengert {1}'.format(len(att_df), len(in_df))
                in_df.to_csv(
                    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_percentOverlap_NonAG\{0}.csv'.format(
                        csv))
            in_df.sort_values(['HUC12'])
            in_df['HUC12'] = in_df['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
            in_sum = in_df.groupby(by=['HUC12','Acres_prj']).sum()
            in_sum.reset_index( inplace=True)

            in_df = in_sum
            in_df.to_csv(final_in_csv)

            acres_col = in_df[('Acres_prj')].map(lambda x: x).astype(float)
            in_df['Acres_prj'] = in_df[('Acres_prj')].map(lambda x: x).astype(str)
            percent_overlap, acres_overlap, msq_overlap = calculation(acres_col, in_df, 30)
            percent_overlap.to_csv(final_csv)
            acres_overlap.to_csv(final_acres_csv)
            msq_overlap.to_csv(final_msq_csv)
        else:
            pass
            # print 'Already exported file {0}'.format(final_csv)
