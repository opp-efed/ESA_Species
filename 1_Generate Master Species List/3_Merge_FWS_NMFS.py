import datetime
import os

import pandas as pd

# #################### VARIABLES
# #### user input variables
outlocation = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\September2017'   # path final tables
current_NMFS_csv = outlocation+ os.sep+'NMFS' +os.sep + 'Full_NMFS_Listed_STD_20170410.csv'
current_FWS = outlocation+ os.sep+'FWS' +os.sep + 'FilteredTessPandas_20170928.csv'

current_masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'

out_cols = ['EntityID', 'Notes', 'comname', 'sciname', 'invname', 'status', 'status_text', 'pop_abbrev',
            'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country', 'Group']


# ####static default variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

current_nmfs_df = pd.read_csv(current_NMFS_csv )
[current_nmfs_df.drop(v, axis=1, inplace=True) for v in current_nmfs_df if v.startswith ('Unnamed')]
current_nmfs_df = current_nmfs_df .reindex(columns=out_cols)
current_nmfs_df['EntityID'] = current_nmfs_df['EntityID'].map(lambda x: x).astype(str)

current_fws_df = pd.read_csv(current_FWS)

fws_cols = current_fws_df.columns.values.tolist()
[current_fws_df.drop(v, axis=1, inplace=True) for v in fws_cols if v.startswith ('Unnamed')]

current_fws_df.columns = out_cols
current_fws_df['EntityID'] = current_fws_df['EntityID'].map(lambda x: x).astype(str)
current_fws_df = current_fws_df.loc[current_fws_df['lead_agency']==1]

current_df= pd.concat([current_fws_df, current_nmfs_df], axis=0)
current_df.to_csv(outlocation + os.sep + 'Full_Merged_Listed' + date + '.csv', encoding='utf-8')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
