import datetime
import os

import pandas as pd

# Internal deliberative, do not cite or distribute
# Author J. Connolly


# path final tables
outlocation = r''

# Download csv of critical habitat table from https://ecos.fws.gov/ecp/report/table/critical-habitat.html
ch_table_ecos = outlocation + os.sep + 'Endangered Species Active Critical Habitat Report.csv'
# file name
new_master = outlocation + os.sep + 'filename.csv'


def check_ch(row, ch_df):
    entid = str(row['EntityID'])
    current_CH_filename = str(row['CH_OriginalFileName'])
    gis_col = str(row['CH_GIS'])

    try:
        ch_file_name = str(ch_df.loc[ch_df['EntityID'] == entid, 'Critical Habitat Shapefile'].iloc[0])

        if ch_file_name == 'nan':
            if gis_col == 'True':
                return 'No Change'

        elif ch_file_name == current_CH_filename:
            return 'No Change'
        else:
            return 'CH needs to be updated'
    except:
        return 'No CritHab'


def update_ch_col_master(row, ch_df, master_df, date_update):
    entid = str(row['EntityID'])
    gis_col = str(row['CH_GIS'])
    des_col = str(row['Des_CH'])

    list_entid_ch_df = ch_df['EntityID'].values.tolist()

    if entid in list_entid_ch_df:
        if str(ch_df.loc[ch_df['EntityID'] == entid, 'Critical Habitat Shapefile'].iloc[0]) == 'nan':
            lead_agency = str(ch_df.loc[ch_df['EntityID'] == entid, 'Lead'].iloc[0])
            if lead_agency == 'NOAA':
                master_df.loc[master_df['EntityID'] == entid, 'Des_CH'] = 'TRUE'
                master_df.loc[master_df['EntityID'] == entid, 'CH_GIS'] = 'FALSE'

            elif lead_agency.startswith('Region'):

                if gis_col == 'True':
                    master_df.loc[master_df['EntityID'] == entid, 'Des_CH'] = 'TRUE'
                    master_df.loc[master_df['EntityID'] == entid, 'CH_GIS'] = 'TRUE'
                else:
                    master_df.loc[master_df['EntityID'] == entid, 'Des_CH'] = 'Not Prudent'
                    master_df.loc[master_df['EntityID'] == entid, 'CH_GIS'] = 'FALSE'
        else:
            master_df.loc[master_df['EntityID'] == entid, 'Des_CH'] = 'TRUE'
            master_df.loc[master_df['EntityID'] == entid, 'CH_GIS'] = 'TRUE'
            if des_col == 'False':
                master_df.loc[master_df['EntityID'] == entid, 'Updated date'] = date_update
                master_df.loc[master_df['EntityID'] == entid, 'Update description'] = 'Crit Hab Desg added'
                master_df.loc[master_df['EntityID'] == entid, 'Source of Call final BE-Critical Habitat'] = ''
                master_df.loc[master_df['EntityID'] == entid, 'CH_Filename'] = ''

        ch_type_fws_table = str(ch_df.loc[ch_df['EntityID'] == entid, 'Critical Habitat Type'].iloc[0])
        master_df.loc[master_df['EntityID'] == entid, 'CH_Type'] = ch_type_fws_table
        master_df.loc[master_df['EntityID'] == entid, 'Critical_Habitat_'] = 'Yes'
    else:
        master_df.loc[master_df['EntityID'] == entid, 'Des_CH'] = 'FALSE'
        master_df.loc[master_df['EntityID'] == entid, 'CH_GIS'] = 'FALSE'
        master_df.loc[master_df['EntityID'] == entid, 'CH_Type'] = 'FALSE'

        master_df.loc[master_df['EntityID'] == entid, 'Critical_Habitat_'] = 'No'


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
update_date = today.strftime('%m/%d/%Y')

ch_table = pd.read_csv(ch_table_ecos, dtype=str)
[ch_table.drop(v, axis=1, inplace=True) for v in ch_table.columns.values.tolist() if v.startswith('Unnamed')]
ch_table['EntityID'] = ch_table['EntityID'].map(lambda x: str(x))

master_list_df = pd.read_csv(new_master, dtype=str)
[master_list_df.drop(v, axis=1, inplace=True) for v in master_list_df.columns.values.tolist() if
 v.startswith('Unnamed')]
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: str(x))

master_list_df['UpdateCritHab'] = master_list_df.apply(lambda row: check_ch(row, ch_table), axis=1)
master_list_df.apply(lambda row: update_ch_col_master(row, ch_table, master_list_df, update_date), axis=1)

[master_list_df.drop(v, axis=1, inplace=True) for v in master_list_df.columns.values.tolist() if
 v.startswith('Unnamed')]
master_list_df.to_csv(outlocation + os.sep + 'MasterListESA_Feb2017_' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
