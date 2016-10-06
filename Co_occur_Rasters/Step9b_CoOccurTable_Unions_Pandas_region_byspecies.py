import datetime
import os

import arcpy
import numpy as np
import pandas as pd

# Title - summarizes by speceis group by species and adds species info to tables

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# date= 20161003
# in_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\tabulated_results\byzone'
# union_gdb = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_Clipped_UnionRange_20160907.gdb'
# # zoneID and the speices found in each zone
# union_fields = ['OBJECTID', 'ZoneSpecies']
#
# # master list
# master_list = 'C:\Users\Admin\Documents\Jen\Workspace\MasterLists\MasterListESA_June2016_20160907.xlsx'
# temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results\byspecies'

# inlocation
date= 20161003
in_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\tabulated_results\byzone'
union_gdb = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_Clipped_UnionRange_20160907.gdb'
# zoneID and the speices found in each zone
union_fields = ['OBJECTID', 'ZoneSpecies']

# master list
master_list = 'C:\Users\Admin\Documents\Jen\Workspace\MasterLists\MasterListESA_June2016_20160907.xlsx'
temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\tabulated_results\byspecies'
group_skip = ['Amphibians','Clams','Corals','Crustaceans','Ferns','Fishes','Flowering']


# TODO set up a dict to read in the use index base on layer name
group_index = 1  # place to extract species group from tablename
SkipUses = []

# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text','Des_CH',	'CH_GIS']
# species groups that can be skipped

# breaks out the intervals into bin

# #####FUNCTIONS
# Not using this function yet but need to include to make sure EntityID is preserved
# Confirms location of EntityID in the masterlist
def ask_user_entity_id(col_from_master):
    list_keys = col_from_master.keys()
    list_keys.sort()

    if col_from_master['EntityID'] != 0:
        user_input = raw_input('What is the column index for the EntityID (base 0): ')
        user_input2 = raw_input('Is the Column Heading for the EntityID [EntityID] Yes or No:')
        if user_input2 == 'Yes':
            col_from_master['EntityID'] = user_input

        else:
            entheading = str(user_input2)
            col_from_master[entheading] = user_input
            if 'EntityID' in col_from_master:
                del col_from_master['EntityID']
    else:
        user_input = 0
    return col_from_master, user_input


# pull information from the columns in the col_included list from the master list and saves into a df
# to be used in output
def extract_species_info(master_in_table, col_from_master):
    check_extention = (master_in_table.split('.'))[1]
    if check_extention == 'xlsx':
        master_list_df = pd.read_excel(master_in_table)
    else:
        master_list_df = pd.read_csv(master_in_table)
    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    ent_list_included = master_list_df['EntityID'].values.tolist()
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    sp_info_header = sp_info_df.columns.values.tolist()

    return sp_info_df, sp_info_header, ent_list_included


# add error code if a speices has not been run or only some of the use have been run

# Generates diction based on union-ed file to determine which zone a species occurs in
def extract_union_if_from_shapes(union_gdb_extract, fc_union_extract, union_fields_extract, zone_species_occurs_union):
    union_ent_dict = {}
    current_species_occurs_union = {}
    in_fc = union_gdb_extract + os.sep + fc_union_extract
    with arcpy.da.SearchCursor(in_fc, union_fields_extract) as cursor:
        for row in cursor:
            row_id = str(row[0])
            ent_list = str(row[1])
            ent_list_final = []
            convert_list = ent_list.split("u")
            for value in convert_list:
                if value == '[':
                    continue
                elif value == ']':
                    continue
                else:
                    t = str(value)
                    t = t.replace("'", "")
                    t = t.replace("]", "")
                    t = t.replace(",", "")
                    t = t.replace(" ", "")
                    ent_list_final.append(t)
            union_ent_dict[row_id] = ent_list_final
            for c in ent_list_final:
                check = zone_species_occurs_union.get(c)
                if check is None:
                    zone_species_occurs_union[c] = [row_id]
                else:
                    check.append(row_id)
                check2 = current_species_occurs_union.get(c)
                if check2 is None:
                    current_species_occurs_union[c] = [row_id]
                else:
                    check2.append(row_id)

        del cursor
    return union_ent_dict, zone_species_occurs_union, current_species_occurs_union


# Takes the group_df_by_zone_sum and summarize by species using the information from above function, zones a species
# occurs in


def add_species_info_overlap(group_df_by_zone_add_sp, ent_list_fc_add_sp_info,
                             zone_species_occurs_add_sp_info, ent_list_master_add_sp_info):
    # TODO Poss errors 3) species does don't occur in this region

    group_df_by_zone_add_sp.drop('Unnamed: 0', axis=1, inplace=True)

    not_in_master = []
    df_sum_overlap = pd.DataFrame()

    group_df_by_zone_add_sp['ZoneID'] = group_df_by_zone_add_sp['ZoneID'].astype(str)
    counter_total = len(ent_list_fc_add_sp_info)
    counter = 0

    while counter < counter_total:
        for v in ent_list_fc_add_sp_info:
            if counter in np.arange(0, (counter_total + 1), 25):
                print 'Completed species {0} remaining {1}...'.format(counter, (counter_total - counter))
            if v not in ent_list_master_add_sp_info:
                counter += 1
                not_in_master.append(v)
            else:
                zones = zone_species_occurs_add_sp_info[v]

                # these  two takes the longest when there are many zone a species flips between which one
                zone_df = group_df_by_zone_add_sp[group_df_by_zone_add_sp['ZoneID'].isin(zones) == True]

                sum_zone = zone_df.sum(axis=0)

                df_sum = pd.DataFrame(data=sum_zone).T  # The T transforms so this is in wide format
                df_sum.drop('ZoneID', axis=1, inplace=True)

                numeric_cols= df_sum.columns.values.tolist()
                df_sum.insert(0, 'EntityID', v)

                ##NOTE IF NUMBER ARE NOT ALIGIN TO CORRECT COLUMNS IS IS PROBABALY HAPPEN WHEN SPECIES INFO + ZONE SUM
                ##ADDED TO MASTER OVERLAP HERE

                df_sum_overlap = pd.concat([df_sum_overlap, df_sum])

                #df_sum_overlap = pd.concat([df_sum_overlap, df_sum])

                counter += 1

                del df_sum, zones, zone_df, sum_zone

    return df_sum_overlap, not_in_master


# add error code if a speices has not been run or only some of the use have been run
def add_species_info_error(ent_list_fc_error_loop, zone_species_occurs_error_loop,
                           error_code_error_loop, sp_header_error_loop, group_df_by_zone_add_sp, group, df_sum_overlap):
    group_df_by_zone_add_sp['TableID'] = group_df_by_zone_add_sp['TableID'].astype(str)
    counter_total = len(ent_list_fc_error_loop)
    counter = 0
    while counter < counter_total:
        for v in ent_list_fc_error_loop:
            if counter in np.arange(0, (counter_total + 1), 5):
                print 'Completed species {0} in error group remaining {1}...'.format(counter, (counter_total - counter))

            df_species = pd.DataFrame(columns=sp_header_error_loop)

            df_species.loc[0, 'EntityID'] = v
            df_species.loc[0, 'Group'] = group
            df_species = df_species.fillna(error_code_error_loop)

            zones = zone_species_occurs_error_loop[v]
            zone_df = group_df_by_zone_add_sp[group_df_by_zone_add_sp['ZoneID'].isin(zones) == True]
            sum_zone = zone_df.sum(axis=0)
            df_sum = pd.DataFrame(data=sum_zone).T  # The T transforms so this is in wide format
            df_sum['EntityID'] = v

            full_sp_df = pd.merge(df_species, df_sum, on='EntityID', how='left')

            counter += 1
            # NOTE IF NUMBER ARE NOT ALIGIN TO CORRECT COLUMNS IS IS PROBABALY HAPPEN WHEN SPECIES INFO + ZONE SUM
            # ADDED TO MASTER OVERLAP HERE

            df_sum_overlap = pd.concat([df_sum_overlap, full_sp_df])
            # df_sum_overlap = df_sum_overlap.append(full_sp_df, ignore_index=True)

            del full_sp_df, df_species, df_sum,  zones, zone_df, sum_zone

    return df_sum_overlap


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Reads in all information for species that should considered
main_out_header = []
sp_table_dict = {}

main_species_infoDF, main_sp_header, ent_list_master = extract_species_info(master_list, col_included)

main_list_tables = os.listdir(in_folder)
main_list_table = [v for v in main_list_tables if v.endswith('.csv')]
for i in main_list_table:
    sp_group = i.split("_")
    sp_group = sp_group[group_index]
    sp_table_dict[sp_group] = i
print sp_table_dict

# Generates the list of union FC so that the ZoneSpecies and ZoneCode can be extracted
arcpy.env.workspace = union_gdb
fc_list = arcpy.ListFeatureClasses()
# print(fc_list)

zoneID_dict = {}
zone_species_dict = {}

for fc in fc_list:
    start_loop = datetime.datetime.now()
    sp_group_list = fc.split('_')

    sp_group = str(sp_group_list[1])

    if sp_group in group_skip:
        continue
    else:
        print "\nWorking on species group {0}".format(sp_group)
        union_id_dict, zone_species_occurs, current_species_occurs = extract_union_if_from_shapes(union_gdb, fc,
                                                                                                  union_fields,
                                                                                                  zone_species_dict)
        ent_list_fc = current_species_occurs.keys()
        zoneID_dict[fc] = union_id_dict
        list_current_zones = union_id_dict.keys()

        del union_id_dict, current_species_occurs

        current_zones_list = sorted(list(map(str, list_current_zones)))
        sp_table = in_folder + os.sep + sp_table_dict[sp_group]
        sp_table_df = pd.read_csv(sp_table)

        df_sumOverlap, not_in_Master = add_species_info_overlap(sp_table_df, ent_list_fc, zone_species_occurs,
                                                                ent_list_master)

        ent_df = df_sumOverlap['EntityID']
        df_sumOverlap.drop(labels=['EntityID'], axis=1, inplace=True)
        df_sumOverlap.insert(0, 'EntityID', ent_df)

        df_sumOverlap = pd.merge(main_species_infoDF, df_sumOverlap, on='EntityID', how='inner')

        print 'Species missing from masters list: {0}'.format(not_in_Master)

        error_code = -77777
        print 'Start adding species information for missing species'
        # check for any species with range file but entID not matching to master list
        if len(not_in_Master) >0:
            df_sumOverlap = add_species_info_error(not_in_Master, zone_species_occurs, error_code,main_sp_header, sp_table_df, sp_group)

        group_overlap_csv = temp_folder + os.sep + str(fc) + "_groupbyspecies_{0}.csv".format(date)
        df_sumOverlap.to_csv(group_overlap_csv)

        end_loop = datetime.datetime.now()
        print "Elapse time {0}".format(end_loop - start_loop)
        del not_in_Master, ent_list_fc, group_overlap_csv

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
