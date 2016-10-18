import pandas as pd
import os
import datetime

in_aqu_huc = 'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\HUC12\Aqu_SpeHUC12.csv'
in_folder ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables\Collapased'
out_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables\filtered_collapased'

def createdirectory(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        print "created directory {0}".format(new_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

createdirectory(out_folder )
in_aqu = pd.read_csv(in_aqu_huc,dtype=str)
in_aqu.drop_duplicates()
huc_12 = in_aqu['HUC12'].values.tolist()

list_csv = os.listdir(in_folder)

for csv in list_csv:
    out_csv = out_folder +os.sep+csv
    in_csv = in_folder+os.sep+csv
    in_df = pd.read_csv(in_csv)
    in_df.drop('Unnamed: 0', axis=1,inplace=True)
    filterd_df = in_df[in_df['HUC12'].isin(huc_12) == True]
    print 'Aqu huc {0} is filter df {1}'.format(len(huc_12),len(filterd_df))
    filterd_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
