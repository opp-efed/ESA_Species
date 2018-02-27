import datetime
import os
import numpy as np
import pandas as pd

# Title - summarizes overlap results by speceis group and the zoneID

in_folder = r'L:\Workspace\ESA_Species\Step3\10417_Files\SETAC_results\Exported_results\RawUse\CSV\Setac_results_patch7'
out_csv = r'L:\Workspace\ESA_Species\Step3\10417_Files\SETAC_results\10147_SumOverlap.csv'
# master list
temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\tabulated_results\byzone'

col_start = 1
labelCol = 0
interval_step = 30
max_dis = 331
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
             'usa': 'GolfCourse'

             }

# breaks out the intervals into bin
bins = np.arange((0 - interval_step), max_dis, interval_step)
print bins


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
def sum_by_interval(in_csv, bins_loop_sum, use):
    in_df = pd.read_csv(in_csv)
    in_df= in_df.fillna(0)
    in_df['LABEL'] = in_df['LABEL'].astype(str)
    in_df['LABEL'] = in_df['LABEL'].map(lambda x: x.replace(',', '')).astype(long)

    binned_df = in_df.groupby(pd.cut(in_df['LABEL'], bins_loop_sum)).sum()  # breaks out into binned intervals

    group_df_by_zone_sum = binned_df.transpose()  # transposes so it is Zones by interval and not interval by zone
    group_df_by_zone_sum = group_df_by_zone_sum.ix[1:]  # removed the summed interval row that is added when binned

    # bin_columns = ['0', '30', '60', '90', '120', '150', '180', '210', '240', '270', '300', '330',
    #                '360', '390',
    #                '420', '450', '480', '510', '540', '570', '600', '630', '660', '690', '720', '750',
    #                '780', '810', '840',
    #                '870', '900', '930', '960', '990', '1020', '1050', '1080', '1110', '1140', '1170',
    #                '1200', '1230', '1260', '1290',
    #                '1320', '1350', '1380', '1410', '1440', '1470', '1500']
    bin_columns = ['0', '30', '60', '90', '120', '150', '180', '210', '240', '270', '300', '330',]
    outcol = []
    for i in bin_columns:
        col = use + "_" + i
        outcol.append(col)
    group_df_by_zone_sum.columns = outcol
    print group_df_by_zone_sum

    return group_df_by_zone_sum


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


list_csv = os.listdir(in_folder)
list_csv = [csv for csv in list_csv if csv.endswith('csv')]
final_df = pd.DataFrame()
for csv_in in list_csv:
    use_lookup = csv_in.split("_")[8]
    if use_lookup == 'CDL':
        use_lookup = csv_in.split("_")[10]
    use = useLookup[use_lookup]
    print use
    csv_path = in_folder + os.sep + csv_in
    use_df = sum_by_interval(csv_path, bins, use)
    final_df = pd.concat([final_df, use_df], axis=1)

final_df.to_csv(out_csv)
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
