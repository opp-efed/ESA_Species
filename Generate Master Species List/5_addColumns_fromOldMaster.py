import datetime
import os

import pandas as pd

# #################### VARIABLES
# #### user input variables

outlocation = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\April2017'  # path final tables
current_masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'
new_master = outlocation + os.sep + 'Full_Merged_Listed_updated_20170410.csv'

# removing inverted name and status
# columns in tables must be in the same order
out_cols = ['EntityID', 'Updated date', 'Update description', 'Notes', 'Delisted', 'Update Agency', 'comname',
            'sciname', 'status_text', 'pop_abbrev', 'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country',
            'Group', 'Entid_Updated']
# Columns to be added dynamically u'DD_Species_DraftBE',u'InBins',u'BinsAssigned', u'Range_Filename',u'CH_Filename',
# Column must be updated dynamically u'CH_OriginalFileName',
col_to_add = [u'NMFSID', u'Des_CH', u'CH_GIS',u'CH_Type', u'CH_OriginalFileName', u'Group b', u'Group c',
              u'Aqu_Species_20160819', u'Source of Call final BE-Range', u'WoE Summary Group',
              u'Source of Call final BE-Critical Habitat',u'Critical_Habitat_', u'Migratory', u'Migratory_',
              u'AddedAquWoe',	u'AddedTerrWoE']

entid_update = {'11356': '5623',
                '11355': 'NMFS180',
                '11353': 'NMFS181'}
entid_updates_list = entid_update.keys()


def extract_value(row, df_child, entid_up_list, entid_up_dict, col):
    entid = str(row['EntityID'])
    if entid in entid_up_list:
        entid = str(entid_up_dict[entid])
    try:
        col_value = str(df_child.loc[df_child['EntityID'] == entid, col].iloc[0])
        return col_value
    except IndexError:
        return ''


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

current_listed_df = pd.read_csv(new_master)
[current_listed_df.drop(v, axis=1, inplace=True) for v in current_listed_df.columns.values.tolist() if v.startswith ('Unnamed')]
current_listed_df = current_listed_df.reindex(columns=out_cols)
current_listed_df['EntityID'] = current_listed_df['EntityID'].map(lambda x: str(x))

master_list_df = pd.read_excel(current_masterlist)
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: str(x))
# print master_list_df.columns.values.tolist()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


for value in col_to_add:
    current_listed_df[value] = current_listed_df.apply(
        lambda row: extract_value(row, master_list_df, entid_updates_list, entid_update, value), axis=1)
    print 'Completed column {0}'.format(value)

current_listed_df.fillna('')

current_listed_df.to_csv(outlocation + os.sep + 'MasterListESA_Feb2017_NeedCH_' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
