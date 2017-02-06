import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species


# Per conversation with Steve on 2/1/2017 the cattle ear tag layer is use to represent pasture in HI, he feels the CCAP data is more
# Accurate than the state data
# inlocation
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\FinalBETables\CriticalHabitat\BE_Intervals\CH_AllUses_BE_20170117.csv'
temp_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Overlap Tables'

chem_name = 'Methomyl'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
out_csv = temp_folder + os.sep + 'CH_FinalBE_' + chem_name + '_Overlap_' + date + '.csv'
out_summary_csv = temp_folder + os.sep + 'CH_FinalBE_' + chem_name + '_summary_' + date + '.csv'

sp_index_cols = 15
col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Group', 'Des_CH',
               'Critical_Habitat_', 'CH_GIS', 'Migratory', 'Migratory_', 'Source of Call final BE-Range',
               'WoE Summary Group', 'Source of Call final BE-CH', 'Bermuda Grass', 'Corn', 'Flybait',
               'Orchards and Vineyards', 'Other Crops', 'Other Grains', 'Other RowCrops', 'Pasture', 'Soybeans',
               'Vegetables and Ground Fruit', 'Wheat', 'Corn_Buffer', 'Orchards and Vineyards_Buffer',
               'Other Crops_Buffer', 'Other Grains_Buffer', 'Other RowCrops_Buffer', 'Pasture_Buffer',
               'Soybeans_Buffer', 'Vegetables and Ground Fruit_Buffer', 'Wheat_Buffer', 'Bermuda Grass_Buffer',
               'Fly bait_Buffer', 'Methomyl_ActionArea', 'Step 1 ED', 'Step 1 ED Comment', 'Step 2 ED',
               'Step 2 ED Comment', 'Fly bait Only'
               ]
# None of the NE Extinct speices should have crithav
NE_Extinct = []

NLAA_Extinct = ['16', '23', '64', '77', '93', '100', '105', '109', '191', '1953', '78', '1302', '91']

# cannot discount the species on Mona island off of PR
NLAA_OutsideUse = ['598', '499', '606']
LAA_OutsideUse = ['163', '164', '165', '177']
# Taken care of in the master list
NoGIS = []
CH_NotPrudent = ['11', '1090', '1236', '1237', '1238', '1239', '1525', '2211', '3194', '4248', '7332', '10226']
# TODO these are the list for diaz update based on chem
QualRepot_speceis = ['7', '45', '153', '154', '155', '160', '447', '448', '449', '459', '460', '461', '463', '464',
                     '465', '466', '467', '469', '470', '471', '472', '473', '474', '485', '1769', '2510', '2862',
                     '2891', '3096', '3133', '3199', '3318', '3379', '4719', '5064', '5232', '5623', '5989', '7115',
                     '7134', '8861', '9126', '9707', '9709', '9941', '10144', '10145', '10381', '10485', '10700',
                     '10733', '10734', '10736', '11175', '11176', '11191', '11192', '11193', 'NMFS137', 'NMFS159',
                     'NMFS175', 'NMFS178', 'NMFS180', 'NMFS181']

LAA_QualReport = ['7', '459', '461', '463', '471', '485', '2891', '7115', '9126', '9707', '10485', '11175', '11176',
                  '11191', '11192', '11193', '154', '153']
NLAA_QualReport = ['2510', '5232', '10144', '10145', '460', '474']
# other chem species were 472 and 473 remove so stays TBD
NE_QualReport = []
# TODO these are the list for diaz cattle eartag update based on chem results fly bait
NLAA_Flybait = []

LAA_Flybait = []

DD_Species = ['2', '7', '19', '26', '58', '67', '69', '70', '76', '84', '88', '91', '103', '104', '108', '124',
              '125', '130', '131', '132', '134', '135', '136', '147', '152', '167', '168', '169', '171', '172',
              '173', '176', '180', '182', '187', '189', '191', '194', '196', '197', '202', '204', '205', '206',
              '207', '209', '210', '211', '212', '213', '214', '215', '216', '218', '219', '220', '221', '222',
              '223', '224', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '237', '238',
              '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252',
              '254', '255', '256', '257', '258', '259', '260', '262', '263', '264', '265', '266', '267', '268',
              '269', '270', '271', '272', '273', '274', '275', '276', '277', '278', '279', '280', '281', '282',
              '283', '284', '285', '286', '287', '290', '292', '293', '294', '295', '296', '297', '298', '299',
              '300', '301', '303', '305', '306', '307', '308', '309', '311', '312', '313', '314', '315', '316',
              '317', '318', '319', '320', '321', '322', '323', '324', '325', '326', '327', '328', '329', '330',
              '331', '332', '333', '334', '335', '336', '337', '338', '339', '340', '341', '342', '343', '344',
              '345', '346', '347', '348', '349', '350', '351', '352', '353', '354', '355', '356', '357', '358',
              '359', '360', '361', '362', '363', '364', '365', '366', '367', '368', '369', '370', '371', '372',
              '373', '374', '375', '376', '377', '378', '379', '380', '381', '382', '383', '384', '385', '386',
              '396', '398', '399', '401', '402', '403', '404', '406', '407', '408', '409', '411', '412', '413',
              '414', '415', '416', '417', '418', '435', '439', '441', '445', '453', '454', '475', '477', '478',
              '479', '480', '481', '482', '484', '486', '487', '489', '517', '580', '677', '807', '870', '1064',
              '1199', '1245', '1246', '1247', '1261', '1302', '1358', '1361', '1369', '1380', '1509', '1559',
              '1680', '1707', '1740', '1783', '1849', '1897', '1905', '1934', '1953', '2142', '2144', '2192',
              '2308', '2316', '2448', '2514', '2528', '2561', '2599', '2767', '2842', '2917', '2956', '3226',
              '3271', '3280', '3364', '3398', '3497', '3525', '3596', '3628', '3645', '3654', '3833', '3842',
              '3879', '4042', '4086', '4093', '4112', '4162', '4210', '4248', '4274', '4300', '4326', '4330',
              '4411', '4431', '4437', '4479', '4490', '4496', '4679', '4766', '4799', '4881', '4910', '4992',
              '5065', '5153', '5180', '5232', '5265', '5281', '5362', '5434', '5658', '5714', '5715', '5718',
              '5719', '5815', '5833', '5856', '5981', '6062', '6138', '6220', '6223', '6231', '6297', '6346',
              '6503', '6534', '6557', '6578', '6596', '6620', '6654', '6662', '6739', '6841', '6843', '6966',
              '7091', '7150', '7177', '7332', '7342', '7349', '7363', '7372', '7512', '7590', '7610', '7670',
              '7800', '7816', '7834', '7847', '7855', '7949', '7989', '8172', '8231', '8241', '8278', '8349',
              '8356', '8434', '8442', '8561', '8621', '8765', '8861', '8921', '8962', '9021', '9061', '9220',
              '9432', '9487', '9488', '9489', '9490', '9491', '9492', '9493', '9494', '9495', '9496', '9497',
              '9498', '9499', '9500', '9501', '9502', '9503', '9504', '9505', '9506', '9507', '9694', '9967',
              '9968', '9969', '10037', '10038', '10039', '10052', '10060', '10077', '10124', '10130', '10150',
              '10297', '10298', '10299', '10300', '10301', '10485', '10517', '10910', '11175', '11176', '11191',
              '11192', '11193', '11201', '11262', 'FWS001', 'NMFS166', 'NMFS175']
# TODO these are the list for diaz DD update based on chem
NE_DD = []
NLAA_DD = []

## There is no crithab in AS so removed all AS use layers
collapses_dict = {
    'Bermuda Grass': ['CONUS_Bermuda Grass_0'],
    'Corn': ['AK_Ag_0',  'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0', 'CONUS_Corn_0'],
    'Flybait': ['AK_Developed_0', 'CNMI_Developed_0', 'CONUS_Developed_0', 'GU_Developed_0',
                'HI_Developed_0', 'PR_Developed_0', 'VI_Developed_0'],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_0', 'HI_Ag_0',
                               'PR_Ag_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Other Crops': ['CONUS_Other Crops_0', 'HI_Ag_0', 'PR_Ag_0', 'AK_Ag_0', 'CNMI_Ag_0',
                    'GU_Ag_0', 'VI_Ag_0'],
    'Other Grains': ['CONUS_Other Grains_0', 'HI_Ag_0', 'PR_Ag_0', 'AK_Ag_0',
                     'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Other RowCrops': ['AK_Ag_0',  'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0',
                       'CONUS_Other RowCrops_0'],
    'Pasture':['AK_Pasture/Hay/Forage_0','HI_Cattle Eartag_0','CONUS_Pasture_0',
               'CNMI_Cattle Eartag_0','GU_Cattle Eartag_0','PR_Cattle Eartag_0','VI_Cattle Eartag_0'],
    'Soybeans': ['CONUS_Soybeans_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Vegetables and Ground Fruit': ['CONUS_Vegetables and Ground Fruit_0', 'HI_Ag_0',
                                    'PR_Ag_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Wheat': ['CONUS_Wheat_0', 'AK_Ag_0',  'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Cotton':['CONUS_Cotton_0','AK_Ag_0','CNMI_Ag_0','GU_Ag_0','HI_Ag_0','PR_Ag_0','VI_Ag_0'],
    'Corn_Buffer': ['AK_Ag_765',  'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765',
                    'CONUS_Corn_765'],
    'Orchards and Vineyards_Buffer': ['CONUS_Orchards and Vineyards_765', 'HI_Ag_765',
                                      'PR_Ag_765', 'AK_Ag_765',  'CNMI_Ag_765',
                                      'GU_Ag_765', 'VI_Ag_765'],
    'Other Crops_Buffer': ['CONUS_Other Crops_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765',
                           'CNMI_Ag_765', 'GU_Ag_765', 'VI_Ag_765'],
    'Other Grains_Buffer': ['CONUS_Other Grains_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765',
                             'CNMI_Ag_765', 'GU_Ag_765', 'VI_Ag_765'],
    'Other RowCrops_Buffer': ['AK_Ag_765',  'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                              'VI_Ag_765', 'CONUS_Other RowCrops_765'],
    'Pasture_Buffer': ['AK_Pasture/Hay/Forage_765', 'HI_Cattle Eartag_765', 'CONUS_Pasture_765',
                        'CNMI_Cattle Eartag_765', 'GU_Cattle Eartag_765', 'PR_Cattle Eartag_765',
                       'VI_Cattle Eartag_765'],
    'Soybeans_Buffer': ['CONUS_Soybeans_765', 'AK_Ag_765',  'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                        'PR_Ag_765', 'VI_Ag_765'],
    'Vegetables and Ground Fruit_Buffer': ['CONUS_Vegetables and Ground Fruit_765', 'HI_Ag_765',
                                           'PR_Ag_765', 'AK_Ag_765',  'CNMI_Ag_765',
                                           'GU_Ag_765', 'VI_Ag_765'],
    'Wheat_Buffer': ['CONUS_Wheat_765', 'AK_Ag_765',  'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                     'VI_Ag_765'],
    'Bermuda Grass_Buffer': ['CONUS_Bermuda Grass_765'],
    'Fly bait_Buffer': ['AK_Developed_765',  'CNMI_Developed_765', 'CONUS_Developed_765',
                        'GU_Developed_765','HI_Developed_765', 'PR_Developed_765', 'VI_Developed_765'],
    'Cotton_Buffer':['CONUS_Cotton_765','AK_Ag_765','CNMI_Ag_765','GU_Ag_765','HI_Ag_765','PR_Ag_765','VI_Ag_765'],
    'Methomyl_ActionArea': ['AK_Methomyl_AA_765',  'CNMI_Methomyl_AA_765', 'CONUS_Methomyl_AA_765',
                            'GU_Methomyl_AA_765', 'HI_Methomyl_AA_765', 'PR_Methomyl_AA_765', 'VI_Methomyl_AA_765'],}

def step_1_ED(row):
    if row['Des_CH'] == 'Not Prudent':
        return 'NE-CritHab Found Not Prudent'
    elif row['Des_CH'] == 'FALSE':
        return 'No CritHab'
    elif row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'May Affect-Extinct'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'May Affect-Outside Use'
    elif row['EntityID'] in LAA_OutsideUse:
        return 'May Affect-Outside Use'
    elif row['EntityID'] in CH_NotPrudent:
        return 'NE-CritHab Found Not Prudent'
    elif row['CH_GIS'] == 'FALSE':
        return 'May Affect-No GIS'
    elif row['EntityID'] in NLAA_QualReport:
        return 'May Affect-QualReport'
    elif row['EntityID'] in LAA_QualReport:
        return 'May Affect-QualReport'
    elif row['EntityID'] in NE_QualReport:
        return 'NE-QualReport'
    elif row['EntityID'] in NE_DD:
        if row['Methomyl_ActionArea'] == 0:
            return 'NE-Overlap and NE-DD'
        elif row['Methomyl_ActionArea'] > 0:
            return 'May Affect-Overlap and NE-DD'
    elif row['EntityID'] in DD_Species:
        if row['Methomyl_ActionArea'] == 0:
            return 'May Affect-DD and NE- Overlap'
        elif row['Methomyl_ActionArea'] > 0:
            return 'May Affect-Overlap and May Affect-DD'
    elif row['Methomyl_ActionArea'] > 0:
        return 'May Affect-Overlap'
    elif row['Methomyl_ActionArea'] == 0:
        return 'NE- Overlap'


def step_2_ED(row):
    if row['Des_CH'] == 'Not Prudent':
        return 'NE-CritHab Found Not Prudent'
    elif row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'NLAA-Extinct'
    elif row['Step 1 ED'].startswith('NE'):
        return 'NE- Step 1'
    elif row['Des_CH'] == 'FALSE':
        return 'NE-No CritHab'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'NLAA-Outside Use'
    elif row['EntityID'] in LAA_OutsideUse:
        return 'LAA-Outside Use'
    elif row['CH_GIS'] == 'FALSE':
        return 'LAA-No GIS'
    elif row['EntityID'] in NLAA_QualReport:
        return 'NLAA-QualReport'
    elif row['EntityID'] in LAA_QualReport:
        return 'LAA-QualReport'
    elif row['EntityID'] in NE_QualReport:
        return 'NE-QualReport'
    elif row['Corn_Buffer'] > 0.4 and row['Orchards and Vineyards_Buffer'] > 0.4 and row['Other Crops_Buffer'] > 0.4 \
            and row['Other Grains_Buffer'] > 0.4 and row['Other RowCrops_Buffer'] > 0.4 and row['Pasture_Buffer'] > 0.4 \
            and row['Soybeans_Buffer'] > 0.4 and row['Vegetables and Ground Fruit_Buffer'] > 0.4 \
            and row['Wheat_Buffer'] > 0.4 and row['Bermuda Grass_Buffer'] > 0.4:

        if row['EntityID'] not in DD_Species:
            return 'LAA-Overlap'
        else:
            if row['EntityID'] in NE_DD:
                return 'LAA-Overlap and NE-DD'
            elif row['EntityID'] in NLAA_DD:
                return 'LAA-Overlap and NLAA-DD'
            else:
                return 'LAA-Overlap and LAA-DD'

    elif row['Corn_Buffer'] <= 0.4 and row['Orchards and Vineyards_Buffer'] <= 0.4 and row['Other Crops_Buffer'] <= 0.4 \
            and row['Other Grains_Buffer'] <= 0.4 and row['Other RowCrops_Buffer'] <= 0.4 \
            and row['Pasture_Buffer'] <= 0.4 and row['Soybeans_Buffer'] <= 0.4 \
            and row['Vegetables and Ground Fruit_Buffer'] <= 0.4 and row['Wheat_Buffer'] <= 0.4 \
            and row['Bermuda Grass_Buffer'] <= 0.4:
        if row['EntityID'] not in DD_Species:
            return 'NLAA-Overlap'
        else:
            if row['EntityID'] in NE_DD:
                return 'NLAA-Overlap and NE-DD'
            elif row['EntityID'] in NLAA_DD:
                return 'NLAA-Overlap and NLAA-DD'
            else:
                return 'LAA-DD and NLAA-Overlap'
    else:
        return 'LAA-Overlap'


def flybait_tag(row):
    if row['CH_GIS'] == 'TRUE':
        if row['Flybait'] >= 0:
            if row['Corn'] <= 0.4 and row['Orchards and Vineyards'] <= 0.4 and row['Other Crops'] <= 0.4 and row['Other Grains'] <= 0.4 and row['Other RowCrops'] <= 0.4 and row['Pasture'] <= 0.4 and row['Soybeans'] <= 0.4 and row['Vegetables and Ground Fruit'] <= 0.4 and row['Wheat'] <= 0.4 and row['Bermuda Grass'] <= 0.4:
                if row['Step 2 ED Comment'] == 'LAA-Overlap and LAA-DD' or row['Step 2 ED Comment'] == 'NE- Step 1':
                    result= 'No'
                elif row['EntityID'] in NE_Extinct:
                    result= 'No'
                elif row['EntityID'] in NLAA_Extinct:
                    result= 'No'
                elif row['EntityID'] in NLAA_OutsideUse:
                    result = 'No'
                elif row['EntityID'] in NoGIS:
                    result ='No'
                else:
                    result ='Yes'

                if result == 'Yes':
                    if row['Corn_Buffer'] <= 0.4 and row['Orchards and Vineyards_Buffer'] <= 0.4 and row['Other Crops_Buffer'] <= 0.4 and row['Other Grains_Buffer'] <= 0.4 and row['Other RowCrops_Buffer'] <= 0.4 and row['Pasture_Buffer'] <= 0.4 and row['Soybeans_Buffer'] <= 0.4 and row['Vegetables and Ground Fruit_Buffer'] <= 0.4 and row['Wheat_Buffer'] <= 0.4 and row['Bermuda Grass_Buffer'] <= 0.4:
                        return 'Yes'
                    else:
                        return 'Yes- Spray Drift Only'
                else:
                    return 'No'



# TODO Removed all steps other than last else as things are completed
def apply_ed_flaybait(row, column):
    if row['CH_GIS'] == 'TRUE':
        if row['EntityID'] in NLAA_Flybait:
            return 'NLAA-Fly bait'
        elif row['EntityID'] in LAA_Flybait:
            return 'LAA-Fly bait'
        elif row['EntityID'] in DD_Species:
            return 'TBD- DD needs to be run'
        elif row['EntityID'] in QualRepot_speceis:
            return 'TBD- Qual Report needs to be finalized'
        elif row['Fly bait Only'] == 'Yes':
            return 'TBD- Fly bait analysis to be finalized'
        else:
            value = row[column]
            return value
    else:
        if row['Des_CH'] == 'TRUE':
            return 'LAA-No GIS'
        else:
            return row['Step 1 ED Comment']


def clean_up_columns(row, column):
    # if row['Des_CH'] == 'Not Prudent':
    #     return 'NE-CritHab Found Not Prudent'
    if row['Critical_Habitat_'] == 'No':
        return 'No CritHab'
    else:
        value = str(row[column].split('-')[0])
        return value


def summary_table(step1, step2, groups):
    df_summary = pd.DataFrame(data=groups, columns=['WoE Summary Group'])
    df_summary = df_summary.reindex(columns=['WoE Summary Group', 'NE', 'May Affect', 'NLAA', 'LAA', 'TBD'])

    for group in groups:
        print group
        for cols in ['NE', 'May Affect']:
            group_df = step1.loc[step1['WoE Summary Group'] == str(group)]
            try:
                value = group_df.loc[group_df['Step 1 ED'] == cols].iloc[0, 2]
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = value
            except:
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = 0
        for cols in ['NLAA', 'LAA','TBD']:
            group_df = step2.loc[step2['WoE Summary Group'] == str(group)]
            try:
                value = group_df.loc[group_df['Step 2 ED'] == cols].iloc[0, 2]
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = value
            except:
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = 0
    df_summary['Total'] = df_summary['NE'] +df_summary['May Affect']
    df_summary = df_summary.append(df_summary.sum(numeric_only=True,axis=0), ignore_index = True)
    print df_summary
    return df_summary


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_uses = collapses_dict.keys()
print list_uses
# Sets up the intervals that are of interests for each of the uses

sp_table_df = pd.read_csv(in_table, dtype=object)
# print sp_table_df
sp_info_df = sp_table_df.iloc[:, :sp_index_cols]
use_df = sp_table_df.iloc[:, sp_index_cols:]
# print use_df

collapsed_df = pd.DataFrame(data=sp_info_df)

for use in list_uses:
    print use
    binned_col = list(collapses_dict[use])

    if not use == 'Mosquito Control':
        if not use == 'Wide Area Use':
            binned_df = use_df[binned_col]
            # print binned_df
            use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
            collapsed_df[(str(use))] = use_results_df.sum(axis=1)
        else:
            collapsed_df.ix[:, str(use)] = 100
    else:
        collapsed_df.ix[:, str(use)] = 100
# Step 1
collapsed_df['Step 1 ED Comment'] = collapsed_df.apply(lambda row: step_1_ED(row), axis=1)
collapsed_df['Step 1 ED'] = collapsed_df['Step 1 ED Comment'].map(
    lambda x: 'No CritHab' if x.endswith('No CritHab') else x)

collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: step_2_ED(row), axis=1)

collapsed_df['Fly bait Only'] = collapsed_df.apply(lambda row: flybait_tag(row), axis=1)

collapsed_df['Step 1 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 1 ED'), axis=1)
collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: apply_ed_flaybait(row, 'Step 2 ED Comment'),
                                                       axis=1)

collapsed_df['Step 2 ED'] = collapsed_df['Step 2 ED Comment'].map(lambda x: x.split('-')[0])
collapsed_df['Step 2 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 2 ED'), axis=1)

final_df = collapsed_df.reindex(columns=col_reindex)
final_df = final_df.fillna(0)

final_df.to_csv(out_csv)

# set up summary table
summary_count_step1 = collapsed_df[['WoE Summary Group', 'Step 1 ED']].groupby(
    ['WoE Summary Group', 'Step 1 ED']).size()
summary_count_step2 = collapsed_df[['WoE Summary Group', 'Step 2 ED']].groupby(
    ['WoE Summary Group', 'Step 2 ED']).size()

df_step1 = summary_count_step1.to_frame()
df_step1.reset_index(inplace=True)

df_step2 = summary_count_step2.to_frame()
df_step2.reset_index(inplace=True)
groups = sorted(list(set(df_step2['WoE Summary Group'].values.tolist())))
out_summary = summary_table(df_step1, df_step2, groups)
out_summary.to_csv(out_summary_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
