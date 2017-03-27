import datetime
import os
import csv

import pandas as pd
# TODO ADD IN CHECK OF FAMILY and country from currext master to load NMFS entity data

# #################### VARIABLES
# #### user input variables
outlocation = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\test'  # path final tables

current_listed_csv = 'FilteredWebsite_NMFS_Listed20170327.csv'
current_canProposed_csv =  'FilteredWebsite_NMFS_PropCan20170327.csv'

family_group_cross ='C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Family_Group_crosswalk_20170325.csv'
current_masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'
current_FWS = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\Feb2017\FWS\FullTess_20170221.csv'

lead_agency_NMFS = ['2', '3']

current_maxNMFSID = 181

out_cols = ['entity_id', 'Notes', 'comname', 'sciname', 'invname', 'status', 'status_text', 'pop_abbrev',
            'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country', 'Group']

# ####static default variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# ## Functions
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
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    global current_maxNMFSID
    sciname = str(row['Scientific Name'])
    pop = str(row['Population'])
    entid = str(row['entity_id'])
    try:
        current_Tess = fws_current_df.loc[
            (fws_current_df['sciname'] == sciname) & (fws_current_df['pop_abbrev'] == pop), 'entity_id'].iloc[0]

        if current_Tess != entid:
            return current_Tess
        else:
            return entid
    except:
        try:
            current_Tess = fws_current_df.loc[(fws_current_df['sciname'] == sciname) & (
                fws_current_df['pop_abbrev'] == pop + (' DPS')), 'entity_id'].iloc[0]
            if current_Tess != entid:
                return str(current_Tess)
            else:
                return entid
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

def check_family_master(row, master_df):
    entid = str(row['entity_id'])
    try:
        current_Tess = master_df.loc[(master_df['EntityID'] == entid), 'family'].iloc[0]
        return current_Tess
    except:
        return ''
def check_family(row, fws_current_df):
    entid = str(row['entity_id'])
    family = (row['family'])
    fws_current_df['entity_id'] = fws_current_df['entity_id'].map(lambda x: x).astype(str)
    try:
        current_Tess = fws_current_df.loc[(fws_current_df['entity_id'] == entid), 'family'].iloc[0]
        current_Tess= current_Tess.decode('unicode_escape').encode('ascii','ignore')
        if family == current_Tess:
            return family
        elif current_Tess != family:
            if current_Tess.startswith(family):
                return family
            else:
                return current_Tess
    except:
        return family


def check_country_master(row, master_df):
    entid = str(row['entity_id'])
    current_country = str(row['country'])
    try:
        current_Tess = master_df.loc[(master_df['EntityID'] == entid), 'country'].iloc[0]
        if current_Tess!= current_country:
            return current_country
    except:
        return current_country

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


def check_country(row, master_df):
    entid = str(row['entity_id'])
    try:
        current_Tess = master_df.loc[(master_df['EntityID'] == entid), 'country'].iloc[0]
        return current_Tess
    except:
        return '3'


def check_nmfs_group(row):
    nmfs_group = row['Group_B']
    group = row['Group']
    if nmfs_group == group:
        return group
    else:
        if nmfs_group in ['Marine Mammals', 'Pinnipeds', 'Cetaceans']:
            return 'Mammals'
        elif nmfs_group in ['Sea Turtles']:
            return 'Reptiles'
        elif nmfs_group in ['Abalone', 'Marine Invertebrates']:
            return 'Snails'
        elif nmfs_group in ['Plants']:
            return 'Flowering Plants'
        else:
            return nmfs_group


def check_group(row, fws_current_df):
    family = str(row['family']).title()
    try:
        group_value = str(fws_current_df.loc[fws_current_df['family'] == family, 'Group'].iloc[0])
        return group_value
    except:
        pass
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

outlocation = outlocation+os.sep+'NMFS'
current_listed_csv = outlocation +os.sep+current_listed_csv
current_canProposed_csv =outlocation +os.sep+current_canProposed_csv
current_listed_df = pd.read_csv(current_listed_csv)
current_canProposed_csv = pd.read_csv(current_canProposed_csv)
current_NMFS = pd.concat([current_listed_df, current_canProposed_csv], axis=0)

master_list_df = pd.read_excel(current_masterlist)
master_list_df['lead_agency']= master_list_df['lead_agency'].map(lambda x: str(x))
master_list_df['lead_agency']= master_list_df['lead_agency'].map(lambda x: str(x.split('.')[0]))
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: str(x))

FWS_list = pd.read_csv(current_FWS)
FWS_list ['entity_id'] = FWS_list ['entity_id'].map(lambda x: str(x))

fmy_grp_xwalk = pd.read_csv(family_group_cross)

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
current_NMFS['family'] = current_NMFS.apply(lambda row: check_family_master(row, nmfs_species_master), axis=1)
current_NMFS['family'] = current_NMFS.apply(lambda row: check_family(row, FWS_list), axis=1)
current_NMFS['spcode'] = current_NMFS.apply(lambda row: check_spcode(row, FWS_list), axis=1)
current_NMFS['vipcode'] = current_NMFS.apply(lambda row: check_vipcode(row, FWS_list), axis=1)
# default lead agency is 2 or NMFS only
current_NMFS['lead_agency'] = current_NMFS.apply(lambda row: check_lead(row, FWS_list), axis=1)
# use old master so that the FWS errors in TESS are not carried forward
# default country for new species is 3 domestic and foreign
current_NMFS['country'] = current_NMFS.apply(lambda row: check_country(row, nmfs_species_master), axis=1)
current_NMFS['country'] = current_NMFS.apply(lambda row: check_country_master(row, nmfs_species_master), axis=1)
current_NMFS['Group'] = current_NMFS.apply(lambda row: check_group(row,fmy_grp_xwalk), axis=1)
current_NMFS['Group'] = current_NMFS.apply(lambda row: check_nmfs_group(row), axis=1)



current_NMFS_std_col = current_NMFS.reindex(columns=out_cols)
current_NMFS.to_csv(outlocation + os.sep + 'Full_NMFS_Listed' + date + '.csv', encoding='utf-8')
current_NMFS_std_col.to_csv(outlocation + os.sep + 'Full_NMFS_Listed_STD_' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)



print '\nCheck that the following columns are complete before merging with nmfs and ' \
      'checking for updates \n {0} \n Family may need to be added manually'.format(out_cols)
