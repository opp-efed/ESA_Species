import datetime
import os

import arcpy
import numpy as np
import pandas as pd

# Title - summarizes overlap results by speceis group and the zoneID

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

in_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\Ag\CriticalHabitat\Mag_Spray\Transposed_Spray'
out_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\Ag\CriticalHabitat\Mag_Spray\SumSpecies'

union_gdb = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat\CH_Clipped_Union_MAG_20161102.gdb'

# zoneID and the speices found in each zone
union_fields = ['ZoneID', 'ZoneSpecies']

# master list

master_list = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170117.xlsx'
#
# table_sp_group = {'Amphibians': 'r_amphib',
#                   'Arachnids': 'r_arachn',
#                   'Birds': 'r_birds',
#                   'Clams': 'r_clams',
#                   'Conifers': 'r_conife',
#                   'Corals': 'r_corals',
#                   'Crustaceans': 'r_crusta',
#                   'Ferns': 'r_ferns',
#                   'Fishes': 'r_fishe',
#                   'Flowering': 'r_flower',
#                   'Insects': 'r_insect',
#                   'Lichens': 'r_lichen',
#                   'Mammals': 'r_mammal',
#                   'Reptiles': 'r_reptil',
#                   'Snails': 'r_snails'}
table_sp_group = {'Amphibians': 'ch_amphi',
                  'Arachnids': 'ch_arach',
                  'Birds': 'ch_birds',
                  'Clams': 'ch_clams',
                  'Conifers': 'ch_conife',
                  'Corals': 'ch_corals',
                  'Crustaceans': 'ch_crust',
                  'Ferns': 'ch_ferns',
                  'Fishes': 'ch_fishe',
                  'Flowering': 'ch_flowe',
                  'Insects': 'ch_insec',
                  'Lichens': 'ch_lichen',
                  'Mammals': 'ch_mamma',
                  'Reptiles': 'ch_repti',
                  'Snails': 'ch_snail'}
group_index = 1  # place to extract species group from tablename
SkipUses = []
# TODO add lookup for all use based on use file name


# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# species groups that can be skipped
group_skip = []

# Generates diction based on union-ed file to determine which zone a species occurs in
def extract_union_if_from_shapes(union_gdb_extract, fc_union_extract, union_fields_extract, zone_species_occurs_union):
    union_ent_dict = {}
    current_species_occurs_union = {}
    in_fc = union_gdb_extract + os.sep + fc_union_extract
    print in_fc
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


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


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
                zones_double = zone_species_occurs_add_sp_info[v]
                zones = []
                for z in zones_double:
                    i = z.replace('.0', '')
                    zones.append(i)

                # these  two takes the longest when there are many zone a species flips between which one
                zone_df = group_df_by_zone_add_sp[group_df_by_zone_add_sp['ZoneID'].isin(zones) == True]

                sum_zone = zone_df.sum(axis=0)

                df_sum = pd.DataFrame(data=sum_zone).T  # The T transforms so this is in wide format
                df_sum.drop('ZoneID', axis=1, inplace=True)
                df_sum.insert(0, 'EntityID', v)

                ##NOTE IF NUMBER ARE NOT ALIGIN TO CORRECT COLUMNS IS IS PROBABALY HAPPEN WHEN SPECIES INFO + ZONE SUM
                ##ADDED TO MASTER OVERLAP HERE

                df_sum_overlap = pd.concat([df_sum_overlap, df_sum])

                counter += 1

                del df_sum, zones, zone_df, sum_zone

    return df_sum_overlap, not_in_master


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
    #print ent_list_included
    return sp_info_df, sp_info_header, ent_list_included


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
createdirectory(out_folder)
# Reads in all information for species that should considered
main_out_header = []

# Generates the list of union FC so that the ZoneSpecies and TableID can be extracted
arcpy.env.workspace = union_gdb
fc_list = arcpy.ListFeatureClasses()
print(fc_list)

zoneID_dict = {}
zone_species_dict = {}

list_folders = os.listdir(in_folder)
main_species_infoDF, main_sp_header, ent_list_master = extract_species_info(master_list, col_included)

for fc in fc_list:
    print "\n".format(fc)
    start_loop = datetime.datetime.now()
    sp_group = fc.split('_')

    sp_group = str(sp_group[1])
    if sp_group in group_skip:
        continue
    print "Working on species group {0}".format(sp_group)
    union_id_dict, zone_species_occurs, current_species_occurs = extract_union_if_from_shapes(union_gdb, fc,
                                                                                              union_fields,
                                                                                              zone_species_dict)

    ent_list_fc = current_species_occurs.keys()
    zoneID_dict[fc] = union_id_dict
    list_current_zones = union_id_dict.keys()

    del union_id_dict, current_species_occurs

    for folder in list_folders:
        out_location = out_folder + os.sep + folder
        createdirectory(out_location)
        list_csv = os.listdir(in_folder + os.sep + folder)

        list_csv = [csv for csv in list_csv if csv.startswith(table_sp_group[sp_group])]
        for csv in list_csv:
            sp_table = (in_folder + os.sep + folder + os.sep + csv)
            sp_table_df = pd.read_csv(sp_table)
            df_sumOverlap, not_in_Master = add_species_info_overlap(sp_table_df, ent_list_fc, zone_species_occurs,
                                                                    ent_list_master)

            ent_df = df_sumOverlap['EntityID']
            df_sumOverlap.drop(labels=['EntityID'], axis=1, inplace=True)
            df_sumOverlap.insert(0, 'EntityID', ent_df)
            df_sumOverlap = pd.merge(main_species_infoDF, df_sumOverlap, on='EntityID', how='inner')
            group_overlap_csv = out_location + os.sep + csv
            df_sumOverlap.to_csv(group_overlap_csv)
       # print main_species_infoDF
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
