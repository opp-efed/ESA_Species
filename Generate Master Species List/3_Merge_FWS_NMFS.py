import datetime
import os
import csv

import pandas as pd

# #################### VARIABLES
# #### user input variables
outlocation = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017'  # path final tables
current_NMFS_csv = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017\NMFS' \
                     r'\Full_NMFS_Listed_STD_20170324.csv'
current_FWS = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\Feb2017\FWS' \
              r'\FilteredTessPandasbb_20170221.csv'

current_masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170117.xlsx'

out_cols = ['entity_id', 'Notes', 'comname', 'sciname', 'invname', 'status', 'status_text', 'pop_abbrev',
            'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country', 'Group']


# ####static default variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

current_nmfs_df = pd.read_csv(current_NMFS_csv )
current_nmfs_df = current_nmfs_df .reindex(columns=out_cols)
current_fws_df = pd.read_csv(current_FWS)
current_fws_df = current_fws_df.reindex(columns=out_cols)
current_fws_df = current_fws_df.loc[current_fws_df['lead_agency']==1]
current_df= pd.concat([current_fws_df, current_nmfs_df], axis=0)

current_df.to_csv(outlocation + os.sep + 'Full_Merged_Listed' + date + '.csv', encoding='utf-8')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
