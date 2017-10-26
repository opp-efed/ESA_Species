import datetime
import os
import numpy as np
import pandas as pd

# Title - summarizes overlap results by speceis group and the zoneID
pilot_speceis = ['4', '18', '20', '51', '83', '116', '129', '138', '139', '140', '142', '151', '173', '175', '192',
                 '200', '1240', '6097']
in_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\PilotGAP\NonAg'
out_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\PilotGAP\NonAg\SumOverlap_2.csv'
# master list


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
             'Cultivated': 'Cultivated',
             'NonCultivated': 'Non Cultivated',
             'PineSeedOrchards': 'Pineseed Orchards',
             'XmasTrees': 'Xmas Tree',
             'OrchardsVineyards': 'Orchards and vineyards',
             'OtherCrops': 'Other crops',
             'OtherGrains': 'Other grains',
             'Pasture': 'Pasture/Hay/Forage',
             'VegetablesGroundFruit': 'Veg Ground Fruit',
             'Diazinon': 'Diazinon_AA',
             'Carbaryl': 'Carbaryl_AA',
             'Chlorpyrifos': 'Chlorpyrifos_AA',
             'Methomyl': 'Methomyl_AA',
             'Malathion': 'Malathion_AA',
             'usa': 'GolfCourse',
             'bermudagrass2': 'Bermuda Grass'

             }

# breaks out the intervals into bin
bins = np.arange((0 - interval_step), max_dis, interval_step)
print bins

bin_columns = bins.tolist()
bin_columns.remove(bin_columns[0])



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
def sum_by_interval(in_csv, bins_loop_sum, use, entid):
    in_df = pd.read_csv(in_csv)
    in_df = in_df.fillna(0)
    in_df['LABEL'] = in_df['LABEL'].astype(str)
    in_df['LABEL'] = in_df['LABEL'].map(lambda x: x.replace(',', '')).astype(long)
    in_df.drop('TableID', axis=1, inplace=True)

    binned_df = in_df.groupby(pd.cut(in_df['LABEL'], bins_loop_sum)).sum()  # breaks out into binned intervals

    group_df_by_zone_sum = binned_df.transpose()  # transposes so it is Zones by interval and not interval by zone
    group_df_by_zone_sum = group_df_by_zone_sum.ix[2:]  # removed the summed interval row that is added when binned

    outcol = []
    for i in bin_columns:
        col = use + "_" + str(i)
        outcol.append(col)

    group_df_by_zone_sum.columns = outcol
    group_df_by_zone_sum.insert(0, 'EntityID', entid)
    outcol.insert(0,'EntityID')
    group_df_by_zone_sum.columns = outcol

    group_df_by_zone_sum['EntityID'] = group_df_by_zone_sum['EntityID'].map(lambda x: x.replace('VALUE_3', entid)).astype(str)
    return group_df_by_zone_sum


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
final_df = pd.DataFrame()

list_folder = os.listdir(in_folder)


for species in pilot_speceis:
    spe_df = pd.DataFrame([species], columns=['EntityID'])
    print spe_df
    for folder in list_folder:
        folder_path = in_folder + os.sep + folder
        list_csv = os.listdir(folder_path)
        list_csv = [csv for csv in list_csv if csv.endswith('csv') and csv.split("_")[1]==species]
        for csv_in in list_csv:
            print csv_in
            use_lookup = csv_in.split("_")[8]
            if use_lookup == 'CDL':
                use_lookup = csv_in.split("_")[10]
            use = useLookup[use_lookup]

            csv_path = folder_path + os.sep + csv_in
            use_df = sum_by_interval(csv_path, bins, use, species)

            spe_df= pd.merge(spe_df, use_df, on='EntityID', how='left')

    final_df = pd.concat([final_df, spe_df], axis=0)

final_df.reset_index( inplace=True)
final_df.drop('index', axis=1, inplace=True)
final_cols=final_df.columns.values.tolist()
final_cols.remove('EntityID')
final_cols.insert(0,'EntityID')
final_df= final_df.reindex(columns=final_cols)
final_df = final_df.fillna(0)
final_df.to_csv(out_csv)
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
