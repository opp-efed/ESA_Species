import datetime
import os

import arcpy
import numpy as np
import pandas as pd

# Title - summarizes overlap results by speceis group and the zoneID

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
date= 20161003
in_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\results'
union_gdb = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_Clipped_UnionRange_20160907.gdb'
regional_acres_table = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\Range_AssignSpeciesRegions_all_20160908.csv'
# zoneID and the speices found in each zone
union_fields = ['ZoneID', 'ZoneSpecies']
regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CONUS', 'CNMI']
skip_regions = []
# master list
temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\tabulated_results\byzone'

col_start = 1
labelCol = 0
interval_step = 30
max_dis = 1501
use_index = 1  # place to extract use from tablename this is not in a standard position
cdl_index = 3
# TODO set up a dict to read in the use index base on layer name
group_index = 1  # place to extract species group from tablename
SkipUses = []
# TODO add lookup for all use based on use file name

useLookup = {'10x2': 'Corn',
             '20x2': 'Cotton',
             '30x2': 'Rice',
             '40x2': 'Soybean',
             '50x2': 'Wheat',
             '60x2': 'Veg Ground Fruit',
             '70x2': 'Orchards and vineyards',
             '80x2': 'Other grains',
             '90x2': 'Other row crops',
             '100x2': 'Other crops',
             '110x2': 'Pasture/Hay/Forage',
             'Ag': 'Ag',
             'CattleEarTag': 'Cattle Eartag',
             'Developed': 'Developed',
             'ManagedForests': 'Managed Forest',
             'Nurseries': 'Nurseries',
             'OSD': 'OSD',
             'ROW': 'ROW',
             'Rangeland': 'Cattle Eartag',
             'CullPiles': 'Cull Piles',
             'Cultivated_2015': 'Cultivated',
             'NonCultivated_2015': 'Non Cultivated',
             'PineSeedOrchards': 'Pineseed Orchards',
             'XmasTrees': 'Xmas Tree',
             'OrchardsVineyards': 'Orchards and vineyards',
             'OtherCrops': 'Other crops',
             'OtherGrains': 'Other grains',
             'Pasture': 'Pasture/Hay/Forage',
             'VegetablesGroundFruit': 'Veg Ground Fruit',
             'Diazinon' : 'Diazinon_AA',
             'Carbaryl': 'Carbaryl_AA',
             'Chlorpyrifos':'Chlorpyrifos_AA',
             'Methomyl':'Methomyl_AA',
             'Malathion':'Malathion_AA'

             }

regionLookup = {'10x2': ['CONUS'],
                '20x2': ['CONUS'],
                '30x2': ['CONUS'],
                '40x2': ['CONUS'],
                '50x2': ['CONUS'],
                '60x2': ['CONUS'],
                '70x2': ['CONUS'],
                '80x2': ['CONUS'],
                '90x2': ['CONUS'],
                '100x2': ['CONUS'],
                '110x2': ['CONUS'],
                'Ag': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'CattleEarTag': ['CONUS', 'AK'],
                'Developed': ['CONUS', 'AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'ManagedForests': ['CONUS', 'AK', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'Nurseries': ['CONUS', 'AK', 'PR', 'HI', 'VI'],
                'OSD': ['CONUS', 'AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'ROW': ['CONUS', 'AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'Rangeland': [ 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'CullPiles': ['CONUS'],
                'Cultivated_2015': ['CONUS'],
                'NonCultivated_2015': ['CONUS'],
                'PineSeedOrchards': ['CONUS'],
                'XmasTrees': ['CONUS'],
                'OrchardsVineyards': ['HI', 'PR'],
                'OtherCrops': ['HI', 'PR'],
                'OtherGrains': ['HI', 'PR'],
                'Pasture': ['HI'],
                'VegetablesGroundFruit': ['HI', 'PR'],
                'Diazinon': ['AK', 'AS', 'CNMI','CONUS','GU', 'HI', 'PR', 'VI'],
                'Carbaryl': ['AK', 'AS', 'CNMI','CONUS','GU', 'HI', 'PR', 'VI'],
                'Chlorpyrifos': ['AK', 'AS', 'CNMI','CONUS','GU', 'HI', 'PR', 'VI'],
                'Methomyl': ['AK', 'AS', 'CNMI','CONUS','GU', 'HI', 'PR', 'VI'],
                'Malathion': ['AK', 'AS', 'CNMI','CONUS','GU', 'HI', 'PR', 'VI']
                }

# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# species groups that can be skipped
group_skip = ['Amphibians','Clams','Corals','Crustaceans','Ferns','Fishes','Flowering']
# breaks out the intervals into bin
bins = np.arange((0 - interval_step), max_dis, interval_step)



# #####FUNCTION

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
    in_df.drop('TableID', axis=1, inplace=True)

    cnt_cha = len(use_sum)
    transposed_header = [word for word in out_header_sum if word[:cnt_cha] == use_sum]

    binned_df = in_df.groupby(pd.cut(in_df['LABEL'], bins_loop_sum)).sum()  # breaks out into binned intervals

    group_df_by_zone_sum = binned_df.transpose()  # transposes so it is Zones by interval and not interval by zone

    group_df_by_zone_sum = group_df_by_zone_sum.ix[1:]  # removed the summed interval row that is added when binned

    group_df_by_zone_sum.columns = transposed_header

    group_df_by_zone_sum['ZoneID'] = group_df_by_zone_sum.index
    group_df_by_zone_sum['ZoneID'] = group_df_by_zone_sum['ZoneID'].map(lambda x: x.replace('VALUE_', '')).astype(str)

    return group_df_by_zone_sum


# loops through all of the output table and by species group and runs the  sum_by_interval
def loop_out_tables(in_path, in_folder_loop, current_group, use_group_loop, grouped_df, bins_loop, out_header_loop):
    grouped_df['ZoneID'] = grouped_df['ZoneID'].astype(str)
    list_table = os.listdir(in_path + os.sep + in_folder_loop)

    list_table = [t for t in list_table if t.endswith('.csv')]
    completed = False
    for table in list_table:

        parse_fc = table.split("_")
        trunc_group = (parse_fc[group_index]).title()

        if current_group.startswith(trunc_group):
            print table

            # print'{0} table with sp trun name {1}'.format(table, trunc_group)
            group = current_group

            if group in group_skip:
                continue
            else:
                completed = True
                use_result_df = pd.read_csv(in_path + os.sep + in_folder_loop + os.sep + table)
                use_result_df.drop('OID', axis=1, inplace=True)

                use_result_df['LABEL'] = use_result_df['LABEL'].astype(str)
                use_result_df['LABEL'] = use_result_df['LABEL'].map(lambda x: x.replace(',', '')).astype(long)

                # print use_result_df

                grouped_df_loop = sum_by_interval(use_result_df, use_group_loop, bins_loop, out_header_loop)

                grouped_df = pd.merge(grouped_df, grouped_df_loop, on='ZoneID', how='left')
                # takes zone that had zero overlap and therefor are not found in table and replaces with 0 values
                grouped_df = grouped_df .fillna(0)

        else:

            pass

    return grouped_df, completed


# add error code if a speices has not been run or only some of the use have been run

def error_code_out_table(error_code_fun, grouped_df, use_error, all_zone_error, out_header_error):

    cnt_cha = len(use_error)
    col_header_loop = [word for word in out_header_error if word[:cnt_cha] == use_error]

    grouped_df['ZoneID'] = grouped_df['ZoneID'].astype(str)
    df_error = pd.DataFrame(columns=col_header_loop)
    df_error.insert(0, 'ZoneID', all_zone_error)
    df_error[col_header_loop] = np.nan
    df_error = df_error.fillna(error_code_fun)

    group_df_by_zone_error = pd.merge(grouped_df, df_error, on='ZoneID', how='left')

    return group_df_by_zone_error

# Generates diction based on union-ed file to determine which zone a species occurs in
def extract_union_if_from_shapes(union_gdb_extract, fc_union_extract, union_fields_extract):
    union_ent_dict = {}
    in_fc = union_gdb_extract + os.sep + fc_union_extract
    ent_list_final = []
    with arcpy.da.SearchCursor(in_fc, union_fields_extract) as cursor:
        for row in cursor:
            row_id = str(row[0])
            ent_list = str(row[1])
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
        del cursor
    return union_ent_dict, ent_list_final

def check_species_regions (region_location_table, sp_ent_list, region , use):
    check_extention = (region_location_table.split('.'))[1]
    if check_extention == 'xlsx':
        region_table_df = pd.read_excel(region_location_table)
    else:
        region_table_df = pd.read_csv(region_location_table)

    region_table_df['EntityID'] = region_table_df['EntityID'].astype(str)
    filterd_df = region_table_df[region_table_df['EntityID'].isin(sp_ent_list) == True]

    region_df = filterd_df[['EntityID',region]]


    region_df = region_df [region_df[region].isin(['Yes']) == True]


    if len(region_df) == 0:
        errorcode = -55555
    else:
        errorcode = 'Run not Complete'

    return errorcode
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Reads in all information for species that should considered
main_out_header = []

# Sets up the intervals that are of interests for each of the uses
uses = sorted(useLookup.keys())
use_intervals = {}
for use in uses:
    if use.endswith('x2'):
        use_final = 'CDL_1015_' + use
    else:
        use_final = use

    for region in regions:
        if region in skip_regions:
            continue
        check_region = regionLookup[use]
        if region not in check_region:
            continue
        else:
            regional_use = region + "_" + use_final
            use_intervals, main_out_header = set_up_intervals(regional_use, bins, main_out_header, use_intervals)

# Looks for folders of the uses that have been run and generates a list of use that have run

completedUses = {}
main_list_folder = os.listdir(in_folder)
main_list_folder = [v for v in main_list_folder if not v.endswith('.gdb')]

for folder in main_list_folder:
    parse_folder = folder.split("_")
    region = parse_folder[0]
    use = parse_folder[use_index]
    # use_value =use[:-2]
    if use == 'CDL':
        use = parse_folder[cdl_index]
        use_group = useLookup[use]
    elif use == 'Cultivated':
        use_group = useLookup[(use+"_2015")]
    elif use == 'NonCultivated':
        use_group = useLookup[(use+"_2015")]

    else:
        use_group = useLookup[use]
    # folder= folder.replace('_euc','')
    completedUses[folder] = region + "_" + use_group

# Generates the list of union FC so that the ZoneSpecies and TableID can be extracted
arcpy.env.workspace = union_gdb
fc_list = arcpy.ListFeatureClasses()
# print(fc_list)

zoneID_dict = {}

for fc in fc_list:
    start_loop = datetime.datetime.now()
    sp_group = fc.split('_')

    sp_group = str(sp_group[1])
    if sp_group in group_skip:
        continue
    print "\nWorking on species group {0}".format(sp_group)
    union_id_dict, sp_ent_in_fc = extract_union_if_from_shapes(union_gdb, fc, union_fields)

    zoneID_dict[fc] = union_id_dict
    list_current_zones = union_id_dict.keys()

    del union_id_dict
    current_zones_list = sorted(list(map(str, list_current_zones)))
    group_df_by_zone = pd.DataFrame(current_zones_list, columns=['ZoneID'])
    # Summarize by zone
    for use in uses:
        if use.endswith('x2'):
            use_final = 'CDL_1015_' + use
        else:
            use_final = use

        for region in regions:
            if region in skip_regions:
                continue
            check_region = regionLookup[use]
            if region not in check_region:
                continue
            else:
                errorcode = check_species_regions(regional_acres_table,sp_ent_in_fc,region, use)

                regional_use = region + "_" + use_final
                folder = regional_use + '_euc'

                if folder in (completedUses.keys()):


                    # print 'FOLDER IS ' + folder
                    group_df_by_zone, completed_run = loop_out_tables(in_folder, folder, sp_group, regional_use,
                                                                      group_df_by_zone, bins, main_out_header)

                    if completed_run is False:
                        #print "{0} has status {1}".format(regional_use, completed_run)
                        main_error_code = errorcode
                        group_df_by_zone = error_code_out_table(main_error_code, group_df_by_zone, regional_use,
                                                                list_current_zones,
                                                                main_out_header)
                    else:
                        pass

    group_csv = temp_folder + os.sep + str(fc) + "_groupbyzone_{0}.csv".format(date)
    group_df_by_zone.to_csv(group_csv)

    end_loop = datetime.datetime.now()
    print "Elapse time {0}".format(end_loop - start_loop)

end_script= datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)