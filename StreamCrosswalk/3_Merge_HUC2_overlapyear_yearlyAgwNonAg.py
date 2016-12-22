import pandas as pd
import os
import datetime

#TODO SET UP LOOP TO DEAL AITH AA AJND AGGREAGEATE
in_folder_ag = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\YearlyCDL\HUC12_transposed'
in_folder_non_ag = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\NonAg\HUC12_transposed'


outFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\FinalTables\Merged'


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_folder_ag = os.listdir(in_folder_ag)
list_folder_nonag = os.listdir(in_folder_non_ag)
ag_csv= [csv for csv in list_folder_ag if csv.endswith('.csv')]
non_ag_csv = [csv for csv in list_folder_nonag if csv.endswith('.csv')]


non_ag_df = pd.read_csv(in_folder_non_ag +os.sep+non_ag_csv[0])
non_ag_df.drop('Unnamed: 0', axis=1, inplace=True)
non_ag_df=non_ag_df.fillna(0)
non_ag_df = non_ag_df.groupby(['HUC12']).sum()
non_ag_df.reset_index(inplace=True)

# outcsv = outFolder +os.sep+ 'NonAg_'+csv
# final_df=non_ag_df.fillna(0)
# final_df.to_csv(outcsv)
for csv in ag_csv:
    print csv
    in_csv = in_folder_ag + os.sep + csv
    in_df = pd.read_csv(in_csv)
    un_named_cols = in_df.columns.values.tolist()
    un_named_cols = [col for col in un_named_cols if col.startswith('Unnamed')]

    for i in un_named_cols:
        in_df.drop(i, axis=1, inplace=True)
    in_df.fillna(0)
    in_df = in_df.groupby(['HUC12']).sum()
    in_df.reset_index(inplace=True)
    final_df = pd.merge(in_df, non_ag_df, on='HUC12', how='outer')

    dups = final_df.set_index('HUC12').index.get_duplicates()
    print dups

    outcsv = outFolder +os.sep+ 'NonAg_'+csv
    final_df=final_df.fillna(0)
    final_df.to_csv(outcsv)


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)