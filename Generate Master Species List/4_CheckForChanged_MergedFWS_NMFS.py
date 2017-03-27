import datetime
import os

import pandas as pd

# #################### VARIABLES
# #### user input variables

outlocation = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\test'   # path final tables
current_masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'
new_master = outlocation +os.sep+ 'Full_Merged_Listed20170327.csv'

# removing inverted name and status
# columns in tables must be in the same order
out_cols = ['entity_id', 'Updated date', 'Update description', 'Notes', 'Delisted', 'Update Agency', 'comname',
            'sciname','status_text', 'pop_abbrev', 'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country',
            'Group']

current_master_col = [u'EntityID', u'Updated date', u'Update description', u'Notes', u'Delisted', u'Update Agency',
                      u'comname', u'sciname', u'status_text', u'pop_abbrev', u'pop_desc', u'family', u'spcode',
                      u'vipcode', u'lead_agency', u'country', u'Group']

index_pos_country = 15
index_pos_entid = 0
index_pos_pop = 9
index_pos_sci = 7
description_dict = {'comname': 'Common Name',
                    'sciname': 'Scientific Name',
                    'status_text': 'Status',
                    'pop_abbrev': 'Pop name',
                    'pop_desc': 'Pop name',
                    'family': 'Family',
                    'spcode': 'Spcode',
                    'vipcode': 'Vipcode',
                    'lead_agency': 'Lead Agency',
                    'country': 'Country',
                    'Group': 'Group'}


def compare_tables(entityid, count_columns, df_parent, df_child, update_new, date_update):
    new_listing = False
    df_parent[df_parent.columns.values.tolist()[index_pos_entid]] = df_parent[df_parent.columns.values.tolist()[index_pos_entid]].map(lambda x: x).astype(str)
    df_child[df_child.columns.values.tolist()[index_pos_entid]] = df_child[df_child.columns.values.tolist()[index_pos_entid]].map(lambda x: x).astype(str)

    # strip out decimal place from country
    df_parent[df_parent.columns.values.tolist()[index_pos_country]] = df_parent[df_parent.columns.values.tolist()[index_pos_country]].map(lambda x: str(x))
    df_parent[df_parent.columns.values.tolist()[index_pos_country]] = df_parent[df_parent.columns.values.tolist()[index_pos_country]].map(lambda x: str(x.split('.')[0])).astype(str)
    df_child[df_child.columns.values.tolist()[index_pos_country]] = df_child[df_child.columns.values.tolist()[index_pos_country]].map(lambda x: str(x))
    df_child[df_child.columns.values.tolist()[index_pos_country]] = df_child[df_child.columns.values.tolist()[index_pos_country]].map(lambda x: str(x.split('.')[0])).astype(str)

    counter = 6
    while counter < count_columns:
        parent_col = str(df_parent.columns.values.tolist()[counter])
        child_col = str(df_child.columns.values.tolist()[counter])
        parent_value = str(df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, parent_col].iloc[0])

        try:
            child_value = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entityid, child_col].iloc[0])
            old_notes = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entityid, 'Notes'].iloc[0])
            old_delist = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entityid, 'Delisted'].iloc[0])
            update_agency = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entityid, 'Update Agency'].iloc[0])
        except:
            # new species won't be on the old list
            new_listing = True

        if not new_listing:
            if parent_value == child_value:
                if update_new:
                    new_update_date = str(df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'].iloc[0])
                    old_update_date = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'].iloc[0]).split(" ")[0]
                    old_update_description = df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'].iloc[0]
                    if new_update_date == date_update:
                        pass
                    else:
                        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = old_update_description
                        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = old_update_date
                        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Notes'] = old_notes
                        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Delisted'] = old_delist
                        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update Agency'] = update_agency
            else:
                if update_new:
                    new_update_date = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'].iloc[0]
                    if new_update_date != date_update:
                        description = df_parent.columns.values.tolist()[counter]
                        if description == 'pop_desc':
                            pass
                        else:
                            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
                            description = description_dict[description]
                            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
                            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update Agency'] = 'Connolly-EPA'
                    else:
                        description = str(df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'].iloc[0])
                        add_description = df_parent.columns.values.tolist()[counter]
                        if add_description == 'pop_desc':
                            pass
                        else:
                            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
                            add_description = description_dict[add_description]
                            check_pop = description.split(' and ')
                            if add_description in check_pop:
                                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
                            else:
                                description = description + ' and ' + add_description
                                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description

            counter += 1
        else:
            break


def check_values(row, df_parent, df_child, update_new, date_update):
    entid = str(row[df_parent.columns.values.tolist()[0]])
    compare_tables(entid, len(df_parent.columns.values.tolist()), df_parent, df_child, update_new, date_update)
    # print 'Completed species {0}'.format(entid)


def update_entityid_description(lead, df_parent, date_update, old_entityid, entityid, added_tess):
    new_update_date = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'].iloc[0]
    if lead == '1':
        if new_update_date != date_update:
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
            description = 'EntityID updated, previously {0}'.format(old_entityid)
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
        else:
            description = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'].iloc[0]
            description = 'EntityID updated, previously {0} and {1}'.format(old_entityid, description)
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
    if lead != '1':
        if new_update_date != date_update:
            if added_tess:
                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
                description = 'EntityID added to TESS, previously {0}'.format(old_entityid)
                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
            else:
                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
                description = 'EntityID updated, previously {0}'.format(old_entityid)
                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
        else:
            if added_tess:
                description = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'].iloc[0]
                description = 'EntityID added to TESS, previously {0}} and {1}'.format(old_entityid, description)
                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
            else:
                description = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'].iloc[0]
                description = 'EntityID updated, previously {0} and {1}'.format(old_entityid, description)
                df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description


def flag_new_species(df_parent, date_update, entityid, added_tess, old_entityid):
    new_update_date = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'].iloc[0]
    df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update Agency'] = 'Connolly-EPA'
    if added_tess:
        if new_update_date != date_update:
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
            description = 'EntityID added to TESS, previously {0}'.format(old_entityid)
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
        else:
            description = df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'].iloc[0]
            description = 'EntityID added to TESS, previously {0}} and {1}'.format(old_entityid, description)
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description
    else:
        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Updated date'] = date_update
        description = 'added'
        df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entityid, 'Update description'] = description


def check_entityID_updates(row, df_parent, df_child, date_update):
    df_parent[df_parent.columns.values.tolist()[index_pos_entid]] = df_parent[df_parent.columns.values.tolist()[index_pos_entid]].map(lambda x: x).astype(str)
    df_child[df_child.columns.values.tolist()[index_pos_entid]] = df_child[df_child.columns.values.tolist()[index_pos_entid]].map(lambda x: x).astype(str)
    child_col = str(df_child.columns.values.tolist()[index_pos_entid])

    sciname = row['sciname']
    pop = row['pop_abbrev']
    entid = row['entity_id']
    added_tess = False
    old_entityid = 'None'

    try:
        child_value = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == entid, child_col].iloc[0])
        if child_value == entid:
            return False
    except:
        try:
            old_entityid = df_child.loc[(df_child['sciname'] == sciname) & (df_child['pop_abbrev'] == pop),child_col].iloc[0]
            added_tess =True

            lead_agency = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == old_entityid,'lead_agency'].iloc[0])
            old_notes = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == old_entityid, 'Notes'].iloc[0])
            old_delist = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == old_entityid, 'Delisted'].iloc[0])
            update_agency = str(df_child.loc[df_child[df_child.columns.values.tolist()[index_pos_entid]] == old_entityid, 'Update Agency'].iloc[0])
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entid, 'Notes'] = old_notes
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entid, 'Delisted'] = old_delist
            df_parent.loc[df_parent[df_parent.columns.values.tolist()[index_pos_entid]] == entid, 'Update Agency'] = update_agency

            update_entityid_description(lead_agency, df_parent, date_update, old_entityid, entid, added_tess)
            return True
        except:
            print 'Entity, sciname and popname change for {0} flag as new species- check'.format(entid)
            flag_new_species(df_parent, date_update, entid, added_tess, old_entityid)
            return True


def check_removed(df_child, df_parent):
    removed_species = []
    old_ent_list = df_child[df_child.columns.values.tolist()[index_pos_entid]].values.tolist()
    new_ent_list = df_parent[df_parent.columns.values.tolist()[index_pos_entid]].values.tolist()
    for entid in old_ent_list:
        if entid in new_ent_list:
            pass
        else:
            removed_species.append(entid)
    return removed_species


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

current_listed_df = pd.read_csv(new_master)
current_listed_df = current_listed_df.reindex(columns=out_cols)
current_listed_df['entity_id'] = current_listed_df['entity_id'].map(lambda x: x).astype(str)

master_list_df = pd.read_excel(current_masterlist)
master_list_df = master_list_df.reindex(columns=current_master_col)
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: x).astype(str)
# print master_list_df.columns.values.tolist()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
update_date = today.strftime('%m/%d/%Y')

current_listed_df.apply(lambda row: check_values(row, current_listed_df, master_list_df, True, update_date), axis=1)
print '\nCompleted species information columns\n'

current_listed_df['Entid_Updated'] = current_listed_df.apply(
    lambda row: check_entityID_updates(row, current_listed_df, master_list_df, update_date), axis=1)
print '\nCompleted species entityid check\n'

removed_list = check_removed(master_list_df, current_listed_df)
print 'Removed Species {0} \nVerify species have be delisted or deemed not warranted'.format(removed_list)
removed_species = master_list_df.loc[master_list_df['EntityID'].isin(removed_list) == True]

added_species= current_listed_df.loc[current_listed_df['Entid_Updated'] == True]
added_species = added_species.loc[added_species['Update description'] == 'added']

current_listed_df.to_csv(outlocation + os.sep + 'Full_Merged_Listed_updated_' + date + '.csv', encoding='utf-8')
removed_species.to_csv(outlocation + os.sep + 'Removed_species_' + date + '.csv', encoding='utf-8')
added_species.to_csv(outlocation + os.sep + 'NewlyListed_species_' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
