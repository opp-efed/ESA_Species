import pandas as pd
import os
import datetime

in_aqu_huc = 'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\HUC12\Aqu_SpeHUC12.csv'
in_folder ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables\Collapased'
out_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables\filtered_collapased'
in_all_Huc12 = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\AllHUC_acres.csv'

def createdirectory(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        print "created directory {0}".format(new_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

createdirectory(out_folder )
in_aqu = pd.read_csv(in_aqu_huc,dtype=str)
in_aqu.drop_duplicates()
in_aqu['HUC12'] = in_aqu['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
huc_12 = in_aqu['HUC12'].values.tolist()
in_aqu.drop_duplicates()
print in_aqu.sort_values(['HUC12'])

list_csv = os.listdir(in_folder)
all_huc12_df = pd.read_csv(in_all_Huc12)
all_huc12_df['HUC12'] = all_huc12_df['HUC_12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
all_huc12_df.drop('HUC_12', axis=1,inplace=True)


for csv in list_csv:
    out_csv = out_folder +os.sep+csv
    in_csv = in_folder+os.sep+csv
    in_df = pd.read_csv(in_csv)
    in_df.drop('Unnamed: 0', axis=1,inplace=True)
    in_df['HUC12'] = in_df['HUC12'].map(lambda x: x).astype(str)
    in_df['HUC12'] = in_df['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
    huc_w_overlap =in_df['HUC12'].values.tolist()
    filterd_df = in_df[in_df['HUC12'].isin(huc_12) == True]

    # Check for aqu HUC12 without any overlap
    nooverlap_huc_list = (in_aqu[in_aqu['HUC12'].isin(huc_w_overlap) == False])
    nooverlap_huc_list =nooverlap_huc_list['HUC12'].values.tolist()
    nooverlap_huc_list = list(set(nooverlap_huc_list))

    nooverlap_huc_df = all_huc12_df [all_huc12_df ['HUC12'].isin(nooverlap_huc_list) == True]
    nooverlap_huc_df =pd.DataFrame(data= nooverlap_huc_df,columns= in_df.columns.values.tolist())


    nooverlap_huc_df.fillna(0)
    final_df = pd.concat([filterd_df,nooverlap_huc_df],axis=0)

    print 'Aqu huc {0} is filter df {1}'.format(len(huc_12),len(final_df))
    final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
