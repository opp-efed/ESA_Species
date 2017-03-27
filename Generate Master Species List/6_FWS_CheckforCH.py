import datetime
import os

import pandas as pd


outlocation = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\test'  # path final tables

# Download csv of critical habitat table from https://ecos.fws.gov/ecp/report/table/critical-habitat.html
ch_table_ecos= outlocation +os.sep+ 'ECOS U.S. FWS Threatened amp Endangered Species Active Critical Habitat Report.csv'
new_master = outlocation + os.sep + 'MasterListESA_Feb2017_NeedCH_20170327.csv'

def check_ch(row, ch_df):
    entid= str(row['entity_id'])
    current_CH_filename= str(row['CH_OriginalFileName'])
    gis_col = str(row['CH_GIS'])

    try:
        ch_file_name = str(ch_df.loc[ch_df['entityId'] == entid, 'Critical Habitat Shapefile'].iloc[0])

        if ch_file_name == 'nan':
            if gis_col =='True':
                return 'No Change'

        elif ch_file_name == current_CH_filename:
            return 'No Change'
        else:
            return 'CH needs to be updated'
    except:
        return 'No CritHab'

def update_ch_col_master(row, ch_df,master_df,date_update):
    entid= str(row['entity_id'])
    gis_col = str(row['CH_GIS'])
    des_col = str(row['Des_CH'])

    list_entid_ch_df = ch_df['entityId'].values.tolist()

    if entid in list_entid_ch_df:
        if str(ch_df.loc[ch_df['entityId'] == entid, 'Critical Habitat Shapefile'].iloc[0]) =='nan':
            lead_agency = str(ch_df.loc[ch_df['entityId'] == entid, 'Lead'].iloc[0])
            if lead_agency =='NOAA':
                master_df.loc[master_df['entity_id'] == entid, 'Des_CH'] = 'TRUE'
                master_df.loc[master_df['entity_id'] == entid, 'CH_GIS'] = 'FALSE'

            elif lead_agency.startswith('Region'):

                if gis_col == 'True':
                    master_df.loc[master_df['entity_id'] == entid, 'Des_CH'] = 'TRUE'
                    master_df.loc[master_df['entity_id'] == entid, 'CH_GIS'] = 'TRUE'
                else:
                    master_df.loc[master_df['entity_id'] == entid, 'Des_CH'] = 'Not Prudent'
                    master_df.loc[master_df['entity_id'] == entid, 'CH_GIS'] = 'FALSE'
        else:
            master_df.loc[master_df['entity_id'] == entid, 'Des_CH'] = 'TRUE'
            master_df.loc[master_df['entity_id'] == entid, 'CH_GIS'] = 'TRUE'
            if des_col =='False':
                master_df.loc[master_df['entity_id'] == entid, 'Updated date'] = date_update
                master_df.loc[master_df['entity_id'] == entid, 'Update description'] = 'Crit Hab Desg added'
                master_df.loc[master_df['entity_id'] == entid, 'Source of Call final BE-Critical Habitat'] = ''
                master_df.loc[master_df['entity_id'] == entid, 'CH_Filename'] = ''




        ch_type_fws_table = str(ch_df.loc[ch_df['entityId'] == entid, 'Critical Habitat Type'].iloc[0])
        master_df.loc[master_df['entity_id'] == entid, 'CH_Type'] = ch_type_fws_table
        master_df.loc[master_df['entity_id'] == entid, 'Critical_Habitat_'] = 'Yes'
    else:
        master_df.loc[master_df['entity_id'] == entid, 'Des_CH'] = 'FALSE'
        master_df.loc[master_df['entity_id'] == entid, 'CH_GIS'] = 'FALSE'
        master_df.loc[master_df['entity_id'] == entid, 'CH_Type'] = 'FALSE'

        master_df.loc[master_df['entity_id'] == entid, 'Critical_Habitat_'] = 'No'


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
update_date = today.strftime('%m/%d/%Y')

ch_table = pd.read_csv(ch_table_ecos, dtype= str)
ch_table['entityId'] = ch_table['entityId'].map(lambda x: str(x))

master_list_df = pd.read_csv(new_master, dtype= str )
master_list_df['entity_id'] = master_list_df['entity_id'].map(lambda x: str(x))

master_list_df['UpdateCritHab'] = master_list_df.apply(lambda row: check_ch(row, ch_table), axis=1)
master_list_df.apply(lambda row: update_ch_col_master(row, ch_table,master_list_df,update_date), axis=1)

[master_list_df.drop(v, axis=1, inplace=True) for v in master_list_df.columns.values.tolist() if v.startswith ('Unnamed')]
master_list_df.to_csv(outlocation + os.sep + 'MasterListESA_Feb2017_' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
