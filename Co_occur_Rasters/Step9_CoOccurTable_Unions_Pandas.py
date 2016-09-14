import datetime
import os

import arcpy
import numpy as np
import pandas as pd


# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
in_folder = 'C:\Workspace\ESA_Species\FinalBE_ForCoOccur\Results_Clipped\Range\CSV'
union_gdb = r'C:\WorkSpace\ESA_Species\FinalBE_ForCoOccur\projectedCLipped.gdb'
# zoneID and the speices found in each zone
union_fields = ['OBJECTID', 'ZoneSpecies']
# master list
master_list = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\April2016\csv' \
              '\input20160801_MasterListESA_June2016_20160725.xlsx'
temp_folder = r'C:\Workspace\ESA_Species\FinalBE_ForCoOccur\Results_Clipped\Range\outputs\SpeciesGroupsTranspose2'
out_csv = temp_folder + os.sep + 'PracticeOverlap_all.csv'

col_start = 1
labelCol = 0
interval_step = 30
max_dis = 1501
use_index = 2  # place to extract use from tablename this is not in a standard position
# TODO set up a dict to read in the use index base on layer name
group_index = 4  # place to extract species group from tablename
SkipUses = []
# TODO add lookup for all use based on use file name

useLookup = {'10': 'Corn',
             '20': 'Cotton',
             '30': 'Rice',
             '40': 'Soybean',
             '50': 'Wheat',
             '60': 'Veg Ground Fruit',
             '70': 'Other trees',
             '80': 'Other grains',
             '90': 'Other row crops',
             '100': 'Other crops',
             '110': 'Pasture/Hay/Forage'
             }
# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text']
# species groups that can be skipped
group_skip = []
# breaks out the intervals into bin
bins = np.arange((0 - interval_step), max_dis, interval_step)


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


# Determines what the intervals are based on the user input
def set_up_intervals(use_int, bins_set_up_interval, out_header_int, use_intervals_dict):
    for i in bins_set_up_interval:
        if i < 0:
            continue
        nm_interval = str(use_int) + '_' + str(i)
        use_interval_list = use_intervals_dict.get(use_int)
        if use_interval_list is None:
            use_intervals_dict[use_int] = [nm_interval]
        else:
            use_interval_list.append(nm_interval)
        if nm_interval not in out_header_int:
            out_header_int.append(nm_interval)

    return use_intervals_dict, out_header_int


# Sums the output to the intervals from input but zoneID and saves it as a DF # By species group
def sum_by_interval(in_df, use_sum, bins_loop_sum, out_header_sum):
    cnt_cha = len(use_sum)
    transposed_header = [word for word in out_header_sum if word[:cnt_cha] == use_sum]
    binned_df = in_df.groupby(pd.cut(in_df['LABEL'], bins_loop_sum)).sum()  # breaks out into binned intervals

    group_df_by_zone_sum = binned_df.transpose()  # transposes so it is Zones by interval and not interval by zone
    group_df_by_zone_sum = group_df_by_zone_sum.ix[1:]  # removed the summed interval row that is added when binned
    group_df_by_zone_sum.columns = transposed_header
    group_df_by_zone_sum['ZoneID'] = group_df_by_zone_sum.index
    group_df_by_zone_sum['ZoneID'] = group_df_by_zone_sum['ZoneID'].map(lambda x: x.replace('Value_', '')).astype(str)

    return group_df_by_zone_sum


# loops through all of the output table and by species group and runs the  sum_by_interval
def loop_out_tables(in_path, in_folder_loop, current_group, use_group_loop, grouped_df, bins_loop, out_header_loop):
    grouped_df['ZoneID'] = grouped_df['ZoneID'].astype(str)
    list_table = os.listdir(in_path + os.sep + in_folder_loop)
    [list_table.remove(word) for word in list_table if word[-3:].lower() != 'csv']
    completed = False
    for table in list_table:
        if table[-3:].lower() != 'csv':
            continue
        parse_fc = table.split("_")
        group = parse_fc[group_index]
        if group in group_skip:
            continue
        elif group == current_group:
            use_result_df = pd.read_csv(in_path + os.sep + in_folder_loop + os.sep + table)
            use_result_df['LABEL'] = use_result_df['LABEL'].map(lambda x: x.replace(',', '')).astype(long)
            grouped_df_loop = sum_by_interval(use_result_df, use_group_loop, bins_loop, out_header_loop)
            grouped_df = pd.merge(grouped_df, grouped_df_loop, on='ZoneID', how='left')
            completed = True
        else:
            continue

    return grouped_df, completed


# add error code if a speices has not been run or only some of the use have been run
def error_code_out_table(error_code_fun, grouped_df, use_error, all_zone_error, out_header_error):
    cnt_cha = len(use_error)
    col_header_loop = [word for word in out_header_error if word[:cnt_cha] == use_error]

    grouped_df['ZoneID'] = grouped_df['ZoneID'].astype(str)

    df_error = pd.DataFrame(columns=col_header_loop)
    df_error.insert(0, 'ZoneID', all_zone_error)
    df_error = df_error.fillna(error_code_fun)

    group_df_by_zone_error = pd.merge(grouped_df, df_error, on='ZoneID', how='left')

    return group_df_by_zone_error


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
def add_species_info_overlap(species_info_df, group_df_by_zone_add_sp, ent_list_fc_add_sp_info,
                             zone_species_occurs_add_sp_info, ent_list_master_add_sp_info, df_sum_overlap):
    # TODO Poss errors 2) it is a sliver or not overlap and should be 0 3) species does don't occur in this region
    species_info_df = species_info_df.loc[species_info_df['EntityID'].isin(ent_list_fc_add_sp_info)]
    not_in_master = []
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

                df_species = species_info_df.loc[species_info_df['EntityID'].isin([v]) == True]

                zones = zone_species_occurs_add_sp_info[v]

                # these  two takes the longest when there are many zone a species flips between which one
                zone_df = group_df_by_zone_add_sp[group_df_by_zone_add_sp['ZoneID'].isin(zones) == True]
                sum_zone = zone_df.sum(axis=0)

                df_sum = pd.DataFrame(data=sum_zone).T  # The T transforms so this is in wide format
                df_sum['EntityID'] = v

                full_sp_df = pd.merge(df_species, df_sum, on='EntityID', how='left')

                header_sp = full_sp_df.columns.values.tolist()
                remove_list = [word for word in header_sp if word not in (df_sum_overlap.columns.values.tolist())]

                for value in remove_list:
                    full_sp_df.drop(value, axis=1, inplace=True)

                counter += 1

                ##NOTE IF NUMBER ARE NOT ALIGIN TO CORRECT COLUMNS IS IS PROBABALY HAPPEN WHEN SPECIES INFO + ZONE SUM
                ##ADDED TO MASTER OVERLAP HERE

                common_col = df_sum_overlap.columns.union(full_sp_df.columns)
                df1 = df_sum_overlap.reindex(columns=common_col)
                df2 = full_sp_df.reindex(columns=common_col)

                df_sum_overlap = pd.concat([df1, df2])

                # df_sum_overlap = df_sum_overlap.append(full_sp_df, ignore_index=True)

                del full_sp_df, df_species, df_sum, remove_list, zones, zone_df, sum_zone, header_sp

    return df_sum_overlap, not_in_master


# add error code if a speices has not been run or only some of the use have been run
def add_species_info_error(ent_list_fc_error_loop, zone_species_occurs_error_loop,
                           error_code_error_loop, sp_header_error_loop, group_df_by_zone_add_sp, group, df_sum_overlap):
    group_df_by_zone_add_sp['ZoneID'] = group_df_by_zone_add_sp['ZoneID'].astype(str)
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

            header_sp = full_sp_df.columns.values.tolist()
            remove_list = [word for word in header_sp if word not in (df_sum_overlap.columns.values.tolist())]
            # print remove_list
            for value in remove_list:
                full_sp_df.drop(value, axis=1, inplace=True)

            counter += 1
            # NOTE IF NUMBER ARE NOT ALIGIN TO CORRECT COLUMNS IS IS PROBABALY HAPPEN WHEN SPECIES INFO + ZONE SUM
            # ADDED TO MASTER OVERLAP HERE

            common_col = df_sum_overlap.columns.union(full_sp_df.columns)
            df1 = df_sum_overlap.reindex(columns=common_col)
            df2 = full_sp_df.reindex(columns=common_col)
            df_sum_overlap = pd.concat([df1, df2])
            # df_sum_overlap = df_sum_overlap.append(full_sp_df, ignore_index=True)

            del full_sp_df, df_species, df_sum, remove_list, zones, zone_df, sum_zone, header_sp

    return df_sum_overlap


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Reads in all information for species that should considered
main_out_header = []

main_species_infoDF, main_sp_header, ent_list_master = extract_species_info(master_list, col_included)
main_out_header.extend(main_sp_header)


# Sets up the intervals that are of interests for each of the uses
uses = sorted(useLookup.values())
use_intervals = {}
for use in uses:
    use_intervals, main_out_header = set_up_intervals(use, bins, main_out_header, use_intervals)

# Looks for folders of the uses that have been run and generates a list of use that have run

completedUses = {}
main_list_folder = os.listdir(in_folder)
for folder in main_list_folder:
    parse_folder = folder.split("_")
    use = parse_folder[use_index]
    # use_value =use[:-2]
    main_use_value = use.replace('x2', '')
    use_group = useLookup[main_use_value]
    completedUses[use_group] = folder

# Generates the list of union FC so that the ZoneSpecies and ZoneCode can be extracted
arcpy.env.workspace = union_gdb
fc_list = arcpy.ListFeatureClasses()
print fc_list
# print(fc_list)


zoneID_dict = {}
zone_species_dict = {}

df_sumOverlap = pd.DataFrame(columns=main_out_header)

for fc in fc_list:
    start_loop = datetime.datetime.now()
    sp_group = fc.split('_')
    sp_group = str(sp_group[0])
    print sp_group
    union_id_dict, zone_species_occurs, current_species_occurs = extract_union_if_from_shapes(union_gdb, fc,
                                                                                              union_fields,
                                                                                              zone_species_dict)

    ent_list_fc = current_species_occurs.keys()
    zoneID_dict[fc] = union_id_dict
    list_zones = union_id_dict.keys()

    del union_id_dict, current_species_occurs

    all_zones_list = sorted(list(map(str, list_zones)))
    group_df_by_zone = pd.DataFrame(all_zones_list, columns=['ZoneID'])

    # Summarize by zone
    for use in uses:
        print use
        if use in (completedUses.keys()):
            folder = completedUses[use]
            group_df_by_zone, completed_run = loop_out_tables(in_folder, folder, sp_group, use, group_df_by_zone, bins,
                                                              main_out_header)
            if completed_run is False:
                main_error_code = -55555
                group_df_by_zone = error_code_out_table(main_error_code, group_df_by_zone, use, all_zones_list,
                                                        main_out_header)
            else:
                continue
        else:
            main_error_code = -99999
            group_df_by_zone = error_code_out_table(main_error_code, group_df_by_zone, use, all_zones_list,
                                                    main_out_header)

    group_df_by_zone = group_df_by_zone.fillna(0)  # zone that are outside use, or drop out because sliver will be
    # updated to a value of 0

    print 'Start adding species information'
    df_sumOverlap, not_in_Master = add_species_info_overlap(main_species_infoDF, group_df_by_zone,
                                                            ent_list_fc, zone_species_occurs, ent_list_master,
                                                            df_sumOverlap)

    print not_in_Master

    error_code = -77777
    print 'Start adding species information for missing species'
    # check for any species with range file but entID not matching to master list
    df_sumOverlap = add_species_info_error(not_in_Master, zone_species_occurs, error_code,
                                           main_sp_header, group_df_by_zone, sp_group, df_sumOverlap)

    # Export temp information
    print 'Start adding Export information'
    group_csv = temp_folder + os.sep + str(fc) + "_groupbyzone.csv"
    group_overlap_csv = temp_folder + os.sep + str(fc) + "_groupbyspecies.csv"
    group_df_by_zone.to_csv(group_csv)
    df_sumOverlap.to_csv(group_overlap_csv)

    end_loop = datetime.datetime.now()
    print "Elapse time {0}".format(end_loop - start_loop)
    del group_df_by_zone, not_in_Master, ent_list_fc, list_zones, group_csv, group_overlap_csv

# check for any species that do  not have range file that was accounted for add them to the list with code -8888
missing_filter = main_species_infoDF[main_species_infoDF['EntityID'].isin(df_sumOverlap['EntityID']) == False]
df_missing = pd.DataFrame(missing_filter, columns=main_out_header)
df_missing = df_missing.fillna(-88888)  # Notes a complete incomplete run
final_df = pd.concat([df_sumOverlap, df_missing])

num = final_df._get_numeric_data()
num[num < -99999] = -55555

final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
