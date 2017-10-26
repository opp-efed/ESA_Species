import datetime
import os

import pandas as pd

# NOTE 2/2/2017: There will be a change moveing for to not include the crop specific layers in HI and PR as a result the
# veg ground fruit, orchard in  vineyard and as a result the cattle ear tag species will not match the later chemicals

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group

# inlocation
in_table = r'E:\Tabulated_NewComps\FinalBETables\CriticalHabitat\BE_Intervals\CH_AllUses_BE_20170109.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# master list
temp_folder = r'E:\Tabulated_NewComps\FinalBETables\Diazinon'
out_csv = temp_folder + os.sep + 'CH_FinalBE_DiazinonOverlap_20170110.csv'
sp_index_cols = 12
col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Group', 'Des_CH',
               'Critical_Habitat_', 'CH_GIS', 'Migratory', 'Migratory_', 'Orchards and Vineyards',
               'Vegetables and Ground Fruit', 'Cattle Eartag', 'Nurseries', 'Nurseries_DiazBuffer',
               'VegetablesGroundFruit_DiazBuffer', 'OrchardsVineyards_DiazBuffer', 'Diazinon_ActionArea',
               'Step 1 ED', 'Step 1 ED Comment', 'Step 2 ED', 'Step 2 ED Comment', 'Cattle Eartag Only'
               ]
NE_Extinct = ['19', '26', '68', '122', '141', '6345', '9433', '9435', '9437', '9445', '9447', '9451', '9455', '9463',
              '9481', '10582']

NLAA_Extinct = ['16', '23', '64', '77', '93', '100', '105', '109', '191', '1953', '78', '1302', '91']

NLAA_CattleEartag = ['34', '54', '57', '185', '199', '266', '280', '281', '282', '283', '284', '285', '406', '431',
                     '432', '446', '496', '497', '506', '507', '514', '523', '533', '538', '548', '559', '560', '561',
                     '576', '581', '594', '614', '621', '626', '630', '634', '635', '645', '648', '659', '660', '671',
                     '673', '674', '685', '686', '710', '717', '767', '769', '775', '779', '780', '782', '827', '832',
                     '840', '869', '871', '884', '888', '916', '917', '919', '928', '939', '952', '958', '961', '981',
                     '993', '1001', '1010', '1020', '1027', '1032', '1050', '1074', '1086', '1092', '1102', '1111',
                     '1117', '1132', '1134', '1136', '1137', '1138', '1140', '1141', '1151', '1154', '1159', '1163',
                     '1174', '1186', '1194', '1196', '1198', '1205', '1207', '1208', '1248', '1249', '1258', '1283',
                     '1349', '1400', '1710', '1831', '1968', '2860', '2934', '4238', '4766', '6617', '7610', '7617',
                     '7805', '9001', '10223', '10228']

LAA_CattleEartag = ['27', '79', '1707', '10517', ]

LAA_AA_error = ['85', '130', '142', '490', '491', '492', '493', '494', '495', '611', '616', '684', '731', '806', '866',
                '1052', '1058', '1126', '1129', '1189', '1202', '1233', '1250', '1256', '1257', '1261', '1989', '3385',
                '4296', '4420', '4565', '6596', '7206', '7229', '8172', '8181', '8336', '10227', '10233', '495'
                ]

LAA_QualReport = ['7', '459', '461', '463', '471', '485', '2891', '7115', '9126', '9707', '10485', '11175', '11176',
                  '11191', '11192', '11193', '154', '153']
NLAA_QualReport = ['2510', '5232', '10144', '10145', '460', '474']

NE_QualReport = ['472', '473']

# check on last 3 that occur on Moa why did we excluded in draft
NLAA_OutsideUse = ['598', '499', '606']
LAA_OutsideUse = ['163','164','165','177',]
CH_NotPrudent = ['11', '1090', '1236', '1237', '1238', '1239', '1525', '2211', '3194', '4248', '7332', '10226']

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
NE_DD = ['58', '147', '196', '209', '215', '234', '249', '255', '256', '259', '263', '264', '266', '273', '274', '275',
         '276', '280', '281', '282', '283', '284', '285', '287', '290', '296', '406', '418', '435', '439', '1245',
         '1246', '1361', '1380', '1707', '1783', '3271', '3280', '4162', '4326', '4479', '4766', '5232', '6138', '6231',
         '6297', '7610', '8172', '8962', '10517', '11192',
         ]
NLAA_DD = ['58', '147', '196', '209', '215', '234', '249', '255', '256', '259', '263', '264', '266', '273', '274',
           '275', '276', '280', '281', '282', '283', '284', '285', '287', '290', '296', '406', '418', '435', '439',
           '1245', '1246', '1361', '1380', '1707', '1783', '3271', '3280', '4162', '4326', '4479', '4766', '5232',
           '6138', '6231', '6297', '7610', '8172', '8962', '10517', '11192',
           ]

# No additional species aew NLAA for DD
collapses_dict = {

    'Vegetables and Ground Fruit': ['CONUS_Vegetables and Ground Fruit_0', 'HI_Veg Ground Fruit_0',
                                    'PR_Veg Ground Fruit_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],

    'Nurseries': ['CONUS_Nurseries_0', 'AK_Nurseries_0', 'HI_Nurseries_0', 'PR_Nurseries_0',
                  'VI_Nurseries_0'],

    'Cattle Eartag': ['CONUS_Cattle Eartag_0', 'AK_Cattle Eartag_0', 'CNMI_Cattle Eartag_0',
                      'GU_Cattle Eartag_0',
                      'HI_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0', ],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_0', 'HI_Orchards and vineyards_0',
                               'PR_Orchards and vineyards_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Nurseries_DiazBuffer': ['CONUS_Nurseries_305', 'AK_Nurseries_305', 'HI_Nurseries_305',
                             'PR_Nurseries_305', 'VI_Nurseries_305'],
    'VegetablesGroundFruit_DiazBuffer': ['CONUS_Vegetables and Ground Fruit_765', 'HI_Veg Ground Fruit_765',
                                         'PR_Veg Ground Fruit_765', 'CNMI_Ag_765', 'AK_Ag_765', 'GU_Ag_765',
                                         'VI_Ag_765', ],
    'OrchardsVineyards_DiazBuffer': ['CONUS_Orchards and Vineyards_305', 'HI_Orchards and vineyards_305',
                                     'PR_Orchards and vineyards_305', 'CNMI_Ag_305', 'AK_Ag_305', 'GU_Ag_305',
                                     'VI_Ag_305', ],
    'Diazinon_ActionArea': ['CONUS_Diazinon_AA_765', 'AK_Diazinon_AA_765', 'CNMI_Diazinon_AA_765',
                            'GU_Diazinon_AA_765', 'HI_Diazinon_AA_765', 'PR_Diazinon_AA_765', 'VI_Diazinon_AA_765',
                            ],

}


def step_1_ED(row):
    if row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'May Affect-Extinct'
    elif row['Des_CH'] == 'FALSE':
        return 'NE-No CritHab'
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
        if row['Diazinon_ActionArea'] == 0:
            if row['EntityID'] in LAA_AA_error:
                return 'May Affect-Overlap and NE-DD'
            else:
                return 'NE-Overlap and NE-DD'
        if row['Diazinon_ActionArea'] > 0:
            return 'May Affect-Overlap and NE-DD'
    elif row['EntityID'] in DD_Species:
        if row['Diazinon_ActionArea'] == 0:
            if row['EntityID'] in LAA_AA_error:
                return 'May Affect-Overlap and May Affect-DD'
            else:
                return 'May Affect-DD and NE- Overlap'
        if row['Diazinon_ActionArea'] > 0:
            return 'May Affect-Overlap and May Affect-DD'
    elif row['Diazinon_ActionArea'] > 0:
        return 'May Affect-Overlap'
    elif row['Diazinon_ActionArea'] == 0:
        if row['EntityID'] in LAA_AA_error:
            return 'May Affect-Overlap'
        else:
            return 'NE- Overlap'


def step_2_ED(row):
    if row['EntityID'] in NE_Extinct:
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
    elif row['Nurseries_DiazBuffer'] > 0.4 and row['VegetablesGroundFruit_DiazBuffer'] > 0.4 and row[
        'OrchardsVineyards_DiazBuffer'] > 0.4 and row['Cattle Eartag'] > 0.4:
        if row['EntityID'] not in DD_Species:
            return 'LAA-Overlap'
        else:
            if row['EntityID'] in NE_DD:
                return 'LAA-Overlap and NE-DD'
            elif row['EntityID'] in NLAA_DD:
                return 'LAA-Overlap and NLAA-DD'
            else:
                return 'LAA-Overlap and LAA-DD'
    elif row['Cattle Eartag'] > 0.4:
        if row['EntityID'] not in DD_Species:
            return 'LAA-Overlap'
        else:
            if row['EntityID'] in NE_DD:
                return 'LAA-Overlap and NE-DD'
            elif row['EntityID'] in NLAA_DD:
                return 'LAA-Overlap and NLAA-DD'
            else:
                return 'LAA-Overlap and LAA-DD'
    elif row['Cattle Eartag'] <= 0.4:
        if row['Nurseries_DiazBuffer'] <= 0.4 and row['VegetablesGroundFruit_DiazBuffer'] <= 0.4 and row[
            'OrchardsVineyards_DiazBuffer'] <= 0.4:
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
    else:
        return 'LAA-Overlap'


def cattle_ear_tag(row):
    if row['Cattle Eartag'] > 0.4:
        if row['Nurseries_DiazBuffer'] <= 0.4 and row['VegetablesGroundFruit_DiazBuffer'] <= 0.4 and row[
            'OrchardsVineyards_DiazBuffer'] <= 0.4:
            if row['Step 2 ED Comment'] == 'LAA-Overlap and LAA-DD' or row['Step 2 ED Comment'] == 'NE- Step 1':
                return 'No'
            elif row['EntityID'] in LAA_AA_error:
                return 'No'
            else:
                return 'Yes'


def apply_ed_cattleeartag(row, column):
    if row['EntityID'] in NLAA_CattleEartag:
        return 'NLAA-Cattle Eartag'
    if row['EntityID'] in LAA_CattleEartag:
        return 'LAA-Cattle Eartag'
    else:
        value = row[column]
        return value


def clean_up_columns(row, column):
    if row['Des_CH'] == 'Not Prudent':
        return 'CritHab Found Not Prudent'
    elif row['Critical_Habitat_'] == 'No':
        return 'No CritHab'
    else:
        value = str(row[column].split('-')[0])
        return value


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

collapsed_df['Cattle Eartag Only'] = collapsed_df.apply(lambda row: cattle_ear_tag(row), axis=1)

collapsed_df['Step 1 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 1 ED'), axis=1)
collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: apply_ed_cattleeartag(row, 'Step 2 ED Comment'),
                                                       axis=1)

collapsed_df['Step 2 ED'] = collapsed_df['Step 2 ED Comment'].map(lambda x: x.split('-')[0])
collapsed_df['Step 2 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 2 ED'), axis=1)

final_df = collapsed_df.reindex(columns=col_reindex)
final_df = final_df.fillna(0)

final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
