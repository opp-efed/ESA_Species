import datetime
import os

import pandas as pd

# NOTE 2/2/2017: There will be a change moveing for to not include the crop specific layers in HI and PR as a result the
# veg ground fruit, orchard in  vineyard and as a result the cattle ear tag species will not match the later chemicals


# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group

# inlocation
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables\R_AllUses_BE_20170209.csv'
temp_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\FinalBETables\MAL_CPY\Malithon'

chem_name = 'Malathion'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
out_csv = temp_folder + os.sep + 'R_DraftBE_' + chem_name + '_Overlap_' + date + '.csv'
out_summary_csv = temp_folder + os.sep + 'R_DraftBE_' + chem_name + '_summary_' + date + '.csv'

sp_index_cols = 15
col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Group', 'Des_CH',
               'Critical_Habitat_', 'CH_GIS', 'Migratory', 'Migratory_', 'Source of Call final BE-Range',
               'WoE Summary Group', 'Source of Call final BE-Critical Habitat', 'Corn', 'Cotton',
               'Developed', 'Nurseries', 'Open Space Developed',
               'Orchards and Vineyards', 'Other Crops', 'Other Grains', 'Other RowCrops', 'Pasture',
               'Rice', 'Soybeans', 'Vegetables and Ground Fruit', 'Wheat', 'Christmas Tree', 'Golfcourses',
               'Right of Way', 'Pine see Orchards', 'Managed Forests', 'Ag',
               'Corn_MalathionBuffer', 'Cotton_MalathionBuffer',
               'Developed_MalathionBuffer', 'Nurseries_MalathionBuffer', 'Open Space Developed_MalathionBuffer',
               'Orchards and Vineyards_MalathionBuffer', 'Other Crops_MalathionBuffer', 'Other Grains_MalathionBuffer',
               'Other RowCrops_MalathionBuffer', 'Pasture_MalathionBuffer',
               'Rice_MalathionBuffer', 'Soybeans_MalathionBuffer',
               'Vegetables and Ground Fruit_MalathionBuffer', 'Wheat_MalathionBuffer', 'Christmas Tree_MalathionBuffer',
               'Golfcourses_MalathionBuffer', 'Right of Way_MalathionBuffer', 'Pine see Orchards_MalathionBuffer',
               'Managed Forests_MalathionBuffer', 'Ag_Buffer', 'Malathion_ActionArea_Aerial',
               'Chlorpyrifos_ActionArea_Aerial', 'Malathion_ActionArea_Ground', 'Chlorpyrifos_ActionArea_Ground',
               'Malathion_ActionArea_noDrift', 'Chlorpyrifos_ActionArea_noDrift',
               'Mosquito Control', 'Wide Area Use',
               'Step 1 ED', 'Step 1 ED Comment', 'Step 2 ED', 'Step 2 ED Comment', 'Drift Only']

NE_Extinct = ['19', '26', '68', '122', '141', '6345', '9433', '9435', '9437', '9445', '9447', '9451', '9455', '9463',
              '9481', '10582']

NLAA_Extinct = ['16', '23', '64', '77', '93', '100', '105', '109', '191', '1953', '78', '1302', '91']
# check on 70 coming up with area in HI now
NLAA_OutsideUse = ['70', '71', '72', '75', '499', '606']

aa_error = ['7731', '7955', '8166', '439']
NoGIS = ['9407']
wide_area_only = ['439', '7731', '8166', '10319']
QualRepot_speceis = ['7', '45', '153', '154', '155', '160', '447', '448', '449', '459', '460', '461', '463', '464',
                     '465', '466', '467', '469', '470', '471', '472', '473', '474', '485', '1769', '2510', '2862',
                     '2891', '3096', '3199', '3318', '3379', '4719', '5064', '5623', '5989', '7115',
                     '7134', '9126', '9707', '9941', '10144', '10145', '10485', '10700',
                     '10733', '10734', '10736', '11175', '11176', '11191', '11192', '11193', 'NMFS137', 'NMFS159',
                     'NMFS175', 'NMFS178', 'NMFS180', 'NMFS181', '3133', '44232', '8861', '9709', '10381', '5232']

LAA_QualReport = ['10485', '11175', '11176', '11191', '11192', '11193', '155', '160', '5989', '9707', '9941', '9126',
                  '7', '45', '2891', '3318', '7115', 'NMFS159', '469', '470', '472', '473', '447', '448', '449', '459',
                  '460', '461', '463', '464', '465', '466', '467', '471', '474', '485', '5064', '3379', '154', '153',
                  '7134', '2862']

NLAA_QualReport = ['1769', '2510', '3096', '3133', '3199', '4719', '5623', '10144', '10145', '10700', '10733', '10734',
                   '10736', 'NMFS137', '5232', '9709', '10381', 'NMFS175', 'NMFS176', 'NMFS182', '8861',
                   'NMFS178', 'NMFS180', 'NMFS181']

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
NLAA_DD = []  # No additional species aew NLAA for DD
collapses_dict = {
    'Corn': ['CONUS_Corn_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0', ],
    'Cotton': ['CONUS_Cotton_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0',
                               'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Other Crops': ['CONUS_Other Crops_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0',
                    'VI_Ag_0', ],
    'Other Grains': ['CONUS_Other Grains_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0',
                     'VI_Ag_0', ],
    'Other RowCrops': ['CONUS_Other RowCrops_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0',
                       'VI_Ag_0', ],
    'Pasture': ['CONUS_Pasture_0', 'AK_Pasture_0', 'CNMI_Cattle Eartag_0', 'GU_Cattle Eartag_0', 'AS_Cattle Eartag_0',
                'HI_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0'],
    'Rice': ['CONUS_Rice_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0', ],

    'Vegetables and Ground Fruit': ['CONUS_Vegetables and Ground Fruit_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0',
                                    'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0', ],

    'Wheat': ['CONUS_Wheat_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Developed': ['CONUS_Developed_0', 'AK_Developed_0', 'CNMI_Developed_0', 'GU_Developed_0', 'HI_Developed_0',
                  'AS_Developed_0',
                  'PR_Developed_0', 'VI_Developed_0'],

    'Nurseries': ['CONUS_Nurseries_0', 'AK_Nurseries_0', 'HI_Nurseries_0', 'PR_Nurseries_0', 'VI_Nurseries_0'],

    'Open Space Developed': ['CONUS_Open Space Developed_0', 'AK_Open Space Developed_0', 'CNMI_Open Space Developed_0',
                             'GU_Open Space Developed_0', 'HI_Open Space Developed_0', 'PR_Open Space Developed_0',
                             'VI_Open Space Developed_0', 'AS_Open Space Developed_0'],
    'Christmas Tree': ['CONUS_Christmas Trees_0'],
    'Golfcourses': ['CONUS_Golfcourses_0', 'AK_Golfcourses_0', 'GU_Golfcourses_0', 'HI_Golfcourses_0',
                    'PR_Golfcourses_0'],
    'Right of Way': ['CONUS_Right of Way_0', 'AK_Right of Way_0', 'CNMI_Right of Way_0', 'GU_Right of Way_0',
                     'HI_Right of Way_0', 'PR_Right of Way_0', 'VI_Right of Way_0', 'AS_Right of Way_0'],
    'Pine see Orchards': ['CONUS_Pine seed orchards_0'],
    'Managed Forests': ['CONUS_Managed Forests_0', 'AK_Managed Forests_0', 'CNMI_Managed Forests_0',
                        'GU_Managed Forests_0', 'HI_Managed Forests_0', 'PR_Managed Forests_0',
                        'VI_Managed Forests_0'],

    'Ag': ['AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Corn_MalathionBuffer': ['CONUS_Corn_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                             'VI_Ag_765', 'AS_Ag_765'],

    'Cotton_MalathionBuffer': ['CONUS_Cotton_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                               'VI_Ag_765', 'AS_Ag_765'],
    'Orchards and Vineyards_MalathionBuffer': ['CONUS_Orchards and Vineyards_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                               'GU_Ag_765', 'HI_Ag_765',
                                               'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other Crops_MalathionBuffer': ['CONUS_Other Crops_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765',
                                    'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other Grains_MalathionBuffer': ['CONUS_Other Grains_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                     'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765', 'HI_Ag_765',
                                     'PR_Ag_765'],
    'Other RowCrops_MalathionBuffer': ['CONUS_Other RowCrops_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                       'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Pasture_MalathionBuffer': ['CONUS_Pasture_765', 'PR_Cattle Eartag_765', 'CNMI_Cattle Eartag_765',
                                'GU_Cattle Eartag_765',
                                'AK_Pasture_765', 'HI_Cattle Eartag_765', 'VI_Cattle Eartag_765',
                                'AS_Cattle Eartag_765'],
    'Rice_MalathionBuffer': ['CONUS_Rice_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                             'VI_Ag_765', 'AS_Ag_765'],
    'Soybeans_MalathionBuffer': ['CONUS_Soybeans_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                 'PR_Ag_765',
                                 'VI_Ag_765', 'AS_Ag_765'],
    'Vegetables and Ground Fruit_MalathionBuffer': ['CONUS_Vegetables and Ground Fruit_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                                    'GU_Ag_765', 'PR_Ag_765',
                                                    'VI_Ag_765', 'AS_Ag_765', 'HI_Ag_765', ],
    'Wheat_MalathionBuffer': ['CONUS_Wheat_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                              'VI_Ag_765', 'AS_Ag_765'],
    'Developed_MalathionBuffer': ['CONUS_Developed_765', 'AK_Developed_765', 'CNMI_Developed_0', 'GU_Developed_765',
                                  'HI_Developed_765', 'PR_Developed_765', 'VI_Developed_765', 'AS_Developed_765'],

    'Nurseries_MalathionBuffer': ['CONUS_Nurseries_765', 'AK_Nurseries_765', 'HI_Nurseries_765', 'PR_Nurseries_765',
                                  'VI_Nurseries_765'],
    'Open Space Developed_MalathionBuffer': ['CONUS_Open Space Developed_765', 'AK_Open Space Developed_765',
                                             'CNMI_Open Space Developed_765', 'AS_Open Space Developed_765',
                                             'GU_Open Space Developed_765', 'HI_Open Space Developed_765',
                                             'PR_Open Space Developed_765', 'VI_Open Space Developed_765'],
    'Christmas Tree_MalathionBuffer': ['CONUS_Christmas Trees_765'],
    'Golfcourses_MalathionBuffer': ['CONUS_Golfcourses_765', 'AK_Golfcourses_765', 'GU_Golfcourses_765',
                                    'HI_Golfcourses_765',
                                    'PR_Golfcourses_765'],
    'Right of Way_MalathionBuffer': ['CONUS_Right of Way_765', 'AK_Right of Way_765', 'CNMI_Right of Way_765',
                                     'GU_Right of Way_765',
                                     'HI_Right of Way_765', 'PR_Right of Way_765', 'VI_Right of Way_765',
                                     'AS_Right of Way_765'],
    'Pine see Orchards_MalathionBuffer': ['CONUS_Pine seed orchards_765'],
    'Managed Forests_MalathionBuffer': ['CONUS_Managed Forests_765', 'AK_Managed Forests_765',
                                        'CNMI_Managed Forests_765',
                                        'GU_Managed Forests_765', 'HI_Managed Forests_765', 'PR_Managed Forests_765',
                                        'VI_Managed Forests_765'],

    'Ag_Buffer': ['AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765'],
    'Malathion_ActionArea_Aerial': ['CONUS_Malathion_AA_765', 'AK_Malathion_AA_765', 'CNMI_Malathion_AA_765',
                                    'GU_Malathion_AA_765', 'HI_Malathion_AA_765', 'PR_Malathion_AA_765',
                                    'VI_Malathion_AA_765',
                                    'AS_Malathion_AA_765'],
    'Chlorpyrifos_ActionArea_Aerial': ['CONUS_Chlorpyrifos_AA_765', 'AK_Chlorpyrifos_AA_765',
                                       'CNMI_Chlorpyrifos_AA_765',
                                       'GU_Chlorpyrifos_AA_765', 'HI_Chlorpyrifos_AA_765', 'PR_Chlorpyrifos_AA_765',
                                       'VI_Chlorpyrifos_AA_765',
                                       'AS_Chlorpyrifos_AA_765'],
    'Malathion_ActionArea_Ground': ['CONUS_Malathion_AA_305', 'AK_Malathion_AA_305', 'CNMI_Malathion_AA_305',
                                    'GU_Malathion_AA_305', 'HI_Malathion_AA_305', 'PR_Malathion_AA_305',
                                    'VI_Malathion_AA_305',
                                    'AS_Malathion_AA_305'],
    'Chlorpyrifos_ActionArea_Ground': ['CONUS_Chlorpyrifos_AA_305', 'AK_Chlorpyrifos_AA_305',
                                       'CNMI_Chlorpyrifos_AA_305',
                                       'GU_Chlorpyrifos_AA_305', 'HI_Chlorpyrifos_AA_305', 'PR_Chlorpyrifos_AA_305',
                                       'VI_Chlorpyrifos_AA_305',
                                       'AS_Chlorpyrifos_AA_305'],
    'Malathion_ActionArea_noDrift': ['CONUS_Malathion_AA_0', 'AK_Malathion_AA_0', 'CNMI_Malathion_AA_0',
                                     'GU_Malathion_AA_0', 'HI_Malathion_AA_0', 'PR_Malathion_AA_0', 'VI_Malathion_AA_0',
                                     'AS_Malathion_AA_0'],
    'Chlorpyrifos_ActionArea_noDrift': ['CONUS_Chlorpyrifos_AA_0', 'AK_Chlorpyrifos_AA_0', 'CNMI_Chlorpyrifos_AA_0',
                                        'GU_Chlorpyrifos_AA_0', 'HI_Chlorpyrifos_AA_0', 'PR_Chlorpyrifos_AA_0',
                                        'VI_Chlorpyrifos_AA_0',
                                        'AS_Chlorpyrifos_AA_0'],

    'Mosquito Control': [],
    'Wide Area Use': []

}


def clean_up_columns(row, column):
    value = str(row[column].split('-')[0])
    return value


def wide_area_ed(row):
    if row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'NLAA-Extinct'
    elif row['EntityID'] in NLAA_QualReport:
        return 'NLAA-QualReport'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'NLAA-OutsideUse'
    elif row['EntityID'] in LAA_QualReport:
        return 'LAA-QualReport'
    elif row['EntityID'] in wide_area_only:
        return 'NLAA-Wide Area Only'
    else:
        return 'LAA-Overlap'


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


collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: wide_area_ed(row), axis=1)
collapsed_df['Step 2 ED'] = collapsed_df['Step 2 ED Comment'].map(lambda x: x.split('-')[0])
collapsed_df['Step 2 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 2 ED'), axis=1)

final_df = collapsed_df.reindex(columns=col_reindex)
final_df = final_df.fillna(0)
# print sorted(collapsed_df.columns.values.tolist())
# print (collapsed_df.columns.values.tolist())
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
