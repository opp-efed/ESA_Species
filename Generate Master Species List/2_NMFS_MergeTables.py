import datetime
import os
import csv

import pandas as pd

# #################### VARIABLES
# #### user input variables
outlocation = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017\NMFS'  # path final tables
current_listed_csv = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017\NMFS' \
                     r'\FilteredWebsite_NMFS_Listed20170324.csv'
current_canProposed_csv = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017\NMFS' \
                          r'\FilteredWebsite_NMFS_PropCan20170324.csv'
current_masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170117.xlsx'
current_FWS = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\Feb2017\FWS\FullTess_20170221.csv'
lead_agency_NMFS = [2, 3]
global current_maxNMFSID
current_maxNMFSID = 181

out_cols = ['entity_id', 'Notes', 'comname', 'sciname', 'invname', 'status', 'status_text', 'pop_abbrev',
            'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country', 'Group']

# ####static default variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


def assign_entitid(row, nmfs_masterlist_df):
    sciname = str(row['Scientific Name'])
    pop = str(row['Population'])
    try:
        value = nmfs_masterlist_df.loc[
            (nmfs_masterlist_df['sciname'] == sciname) & (nmfs_masterlist_df['pop_abbrev'] == pop), 'EntityID'].iloc[0]
        return value
    except:
        try:
            value = nmfs_masterlist_df.loc[(nmfs_masterlist_df['sciname'] == sciname) & (
                nmfs_masterlist_df['pop_abbrev'] == pop + (' DPS')), 'EntityID'].iloc[0]
            return value
        except:
            pass


def check_entity_currentTESS(row, fws_current_df):
    global current_maxNMFSID
    sciname = str(row['Scientific Name'])
    pop = str(row['Population'])
    entid = str(row['entity_id'])
    try:
        current_Tess = fws_current_df.loc[
            (fws_current_df['sciname'] == sciname) & (fws_current_df['pop_abbrev'] == pop), 'entity_id'].iloc[0]
        if current_Tess != entid:
            return current_Tess
    except:
        try:
            current_Tess = fws_current_df.loc[(fws_current_df['sciname'] == sciname) & (
                fws_current_df['pop_abbrev'] == pop + (' DPS')), 'entity_id'].iloc[0]
            if current_Tess != entid:
                return str(current_Tess)
        except:
            value = entid
            if value == 'None':
                current_maxNMFSID = int(current_maxNMFSID + 1)
                current_Tess = 'NMFS' + str(current_maxNMFSID)
                return str(current_Tess)
            else:
                return entid


def check_commonname(row, fws_current_df):
    entid = str(row['entity_id'])
    nmfs_common = row['Invname']
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'comname'].iloc[0]
        return current_Tess
    except:
        break_common = nmfs_common.split(',')
        if len(break_common) == 1:
            return nmfs_common
        else:
            check_extra_info = break_common[1].split('(')
            if len(check_extra_info) > 1:
                common = check_extra_info[0].replace(',', '').lstrip() + " " + break_common[0]
            else:
                common = break_common[1].replace(',', '').lstrip() + " " + break_common[0]
            return common


def assign_status(row):
    current_status = row['Status']
    if current_status in ['E', 'T']:
        return current_status
    else:
        break_status = current_status.split(' ')
        if len(break_status) == 1:
            return current_status[0].title()
        else:
            return break_status[0][0].title() + break_status[1][0].title()


def assign_status_test(row):
    current_status = row['status']
    if current_status == 'E':
        return 'Endangered'
    elif current_status == 'T':
        return 'Threatened'
    elif current_status == 'C':
        return 'Candidate'
    elif current_status == 'PE':
        return 'Proposed Endangered'
    elif current_status == 'PT':
        return 'Proposed Threatened'

    if current_status in ['E', 'T']:
        return current_status


def check_popdesc(row, fws_current_df):
    entid = str(row['entity_id'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'pop_desc'].iloc[0]
        return current_Tess
    except:
        return 'NOT IN TESS'


def check_family(row, fws_current_df):
    entid = str(row['entity_id'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'family'].iloc[0]
        return current_Tess
    except:
        return ''


def check_spcode(row, fws_current_df):
    entid = str(row['entity_id'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'spcode'].iloc[0]
        return current_Tess
    except:
        return 'NOT IN TESS'


def check_vipcode(row, fws_current_df):
    entid = str(row['entity_id'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'vipcode'].iloc[0]
        return current_Tess
    except:
        return 'NOT IN TESS'


def check_lead(row, fws_current_df):
    entid = str(row['entity_id'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'lead_agency'].iloc[0]
        if int(current_Tess) ==1:
            current_Tess = 3
            return current_Tess
        else:
            return int(current_Tess)
    except:
        return 2


def check_country(row, fws_current_df):
    entid = str(row['entity_id'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'lead_agency'].iloc[0]
        return current_Tess
    except:
        return ''


def check_group(row, fws_current_df):
    nmfs_group = row['Group_B']
    if nmfs_group in ['Marine Mammals', 'Pinnipeds', 'Cetaceans']:
        return 'Mammals'
    elif nmfs_group in ['Sea Turtles']:
        return 'Reptiles'
    elif nmfs_group in ['Abalone', 'Marine Invertebrates']:
        return 'Snails'
    else:
        return nmfs_group


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

current_listed_df = pd.read_csv(current_listed_csv)
current_canProposed_csv = pd.read_csv(current_canProposed_csv)
current_NMFS = pd.concat([current_listed_df, current_canProposed_csv], axis=0)

master_list_df = pd.read_excel(current_masterlist)
FWS_list = pd.read_csv(current_FWS)

nmfs_species_master = master_list_df.loc[master_list_df['lead_agency'].isin(lead_agency_NMFS) == True]

current_NMFS['entity_id'] = current_NMFS.apply(lambda row: assign_entitid(row, nmfs_species_master), axis=1)
current_NMFS['entity_id'] = current_NMFS.apply(lambda row: check_entity_currentTESS(row, FWS_list), axis=1)
current_NMFS['comname'] = current_NMFS.apply(lambda row: check_commonname(row, FWS_list), axis=1)
current_NMFS['sciname'] = current_NMFS['Scientific Name'].map(lambda x: x).astype(str)
current_NMFS['invname'] = current_NMFS['Invname'].map(lambda x: x).astype(str)
current_NMFS['status'] = current_NMFS.apply(lambda row: assign_status(row), axis=1)
current_NMFS['status_text'] = current_NMFS.apply(lambda row: assign_status_test(row), axis=1)
current_NMFS['pop_abbrev'] = current_NMFS['Population'].map(lambda x: x).astype(str)
current_NMFS['pop_desc'] = current_NMFS.apply(lambda row: check_popdesc(row, FWS_list), axis=1)
current_NMFS['family'] = current_NMFS.apply(lambda row: check_family(row, FWS_list), axis=1)
current_NMFS['vipcode'] = current_NMFS.apply(lambda row: check_vipcode(row, FWS_list), axis=1)
current_NMFS['lead_agency'] = current_NMFS.apply(lambda row: check_lead(row, FWS_list), axis=1)
current_NMFS['country'] = current_NMFS.apply(lambda row: check_country(row, FWS_list), axis=1)
current_NMFS['Group'] = current_NMFS.apply(lambda row: check_group(row, FWS_list), axis=1)
current_NMFS['spcode'] = current_NMFS.apply(lambda row: check_spcode(row, FWS_list), axis=1)

# att_df['HUC_12'] = att_df['HUC_12'].map(lambda x: '0'+x if len(x) == 11 else x).astype(str)
current_NMFS_std_col = current_NMFS.reindex(columns=out_cols)
current_NMFS.to_csv(outlocation + os.sep + 'Full_NMFS_Listed' + date + '.csv', encoding='utf-8')
current_NMFS_std_col.to_csv(outlocation + os.sep + 'Full_NMFS_Listed_STD_' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
