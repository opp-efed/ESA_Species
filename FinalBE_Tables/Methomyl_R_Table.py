import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# Per conversation with Steve on 2/1/2017 the cattle ear tag layer is use to represent pasture in HI, he feels the CCAP data is more
# Accurate than the state data
# figure out why 589	954	1175	1218	3540	7229 are coming yp NLAA fly bait and overlap should just be flybait? because it include driift?

# inlocation
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables\R_AllUses_BE_20170209.csv'
temp_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Overlap Tables'

chem_name = 'Methomyl'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
out_csv = temp_folder + os.sep + 'R_DraftBE_' + chem_name + '_Overlap_' + date + '.csv'
out_summary_csv = temp_folder + os.sep + 'R_DraftBE_' + chem_name + '_summary_' + date + '.csv'

sp_index_cols = 15
col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Group', 'Des_CH',
               'Critical_Habitat_', 'CH_GIS', 'Migratory', 'Migratory_', 'Source of Call final BE-Range',
               'WoE Summary Group', 'Source of Call final BE-Critical Habitat', 'Bermuda Grass', 'Corn', 'Flybait',
               'Orchards and Vineyards', 'Other Crops', 'Other Grains', 'Other RowCrops', 'Pasture', 'Soybeans',
               'Vegetables and Ground Fruit', 'Methomyl Wheat', 'Cotton', 'Alley Cropping', 'Corn_Buffer',
               'Orchards and Vineyards_Buffer',
               'Other Crops_Buffer', 'Other Grains_Buffer', 'Other RowCrops_Buffer', 'Pasture_Buffer',
               'Soybeans_Buffer', 'Vegetables and Ground Fruit_Buffer', 'Methomyl Wheat_Buffer', 'Bermuda Grass_Buffer',
               'Cotton_Buffer', 'Alley Cropping_Buffer',
               'Fly bait_Buffer', 'Methomyl_ActionArea', 'Step 1 ED', 'Step 1 ED Comment', 'Step 2 ED',
               'Step 2 ED Comment', 'Fly bait Only', 'Drift Only-NonFly bait'
               ]
NE_Extinct = ['19', '26', '68', '122', '141', '6345', '9433', '9435', '9437', '9445', '9447', '9451', '9455', '9463',
              '9481', '10582']

NLAA_Extinct = ['16', '23', '64', '77', '93', '100', '105', '109', '191', '1953', '78', '1302', '91']
# check on 70 coming up with area in HI now
NLAA_OutsideUse = ['70', '71', '72', '75', '499', '606']

NoGIS = ['9407']

# 133.44232.8861.9709.10381 5232 because they are NLAA for overlap too
#  would be NLAA for for overlap and DD but we are leaving it with LAA based on the report
QualRepot_speceis = ['7', '45', '153', '154', '155', '160', '447', '448', '449', '459', '460', '461', '463', '464',
                     '465', '466', '467', '469', '470', '471', '472', '473', '474', '485', '1769', '2510', '2862',
                     '2891', '3096', '3199', '3318', '3379', '4719', '5064', '5623', '5989', '7115',
                     '7134', '9126', '9707', '9941', '10144', '10145', '10485', '10700',
                     '10733', '10734', '10736', '11175', '11176', '11191', '11192', '11193', 'NMFS137', 'NMFS159',
                     'NMFS175', 'NMFS178', 'NMFS180', 'NMFS181', '3133', '44232', '8861', '9709', '10381', '5232']

LAA_QualReport = ['7', '45', '153', '154', '155', '160', '447', '448', '449', '459', '460', '461', '463', '464', '465',
                  '466', '467', '469', '470', '471', '472', '473', '474', '485', '2862', '2891', '3318', '3379', '5064',
                  '5989', '7115', '7134', '9126', '9707', '9941', '10485', '11175', '11176', '11191', '11192', '11193',
                  'NMFS159', ]
#  3133.44232.8861.9709.10381 because they are NLAA for overlap but leaving the call in the report
NLAA_QualReport = ['1769', '2510', '3096', '3199', '4719', '5623', '9709', '10144', '10145', '10733', '10734', '10736',
                   ' NMFS137', '10700', 'NMFS175', 'NMFS178', ' NMFS180', ' NMFS181', '3133', '44232', '8861', '9709',
                   '10381', '5232']

NLAA_Flybait = ['4', '28', '46', '61', '89', '146', '147', '162', '200', '510', '535', '536', '537', '545', '563',
                '565',
                '575', '577', '589', '601', '604', '616', '622', '623', '635', '684', '688', '690', '691', '697', '720',
                '725', '726', '727', '728', '732', '733', '755', '765', '768', '771', '772', '773', '774', '776', '781',
                '801', '814', '815', '833', '839', '846', '847', '848', '851', '864', '865', '895', '915', '918', '938',
                '948', '954', '955', '965', '968', '981', '983', '987', '1016', '1049', '1051', '1052', '1054', '1062',
                '1067', '1071', '1083', '1084', '1097', '1099', '1101', '1102', '1104', '1105', '1109', '1121', '1124',
                '1128', '1129', '1135', '1139', '1144', '1146', '1148', '1152', '1155', '1156', '1163', '1175', '1176',
                '1177', '1179', '1181', '1182', '1183', '1184', '1197', '1201', '1202', '1208', '1211', '1218', '1223',
                '1224', '1227', '1230', '1232', '1278', '1361', '1407', '1521', '1609', '1636', '1968', '2085', '2144',
                '2268', '2364', '2404', '2517', '2619', '2727', '2782', '2970', '3020', '3084', '3175', '3387', '3472',
                '3540', '3653', '3728', '3753', '3876', '4136', '4201', '4297', '4308', '4326', '4377', '4413', '4551',
                '4754', '4858', '5333', '5580', '5956', '6019', '6231', '6303', '6522', '6536', '6632', '6654', '6679',
                '6747', '6845', '6867', '7136', '7170', '7229', '7529', '7840', '7907', '7948', '8338', '8347', '8357',
                '9282', '9395', '9399', '9403', '9405', '9413', '9421', '9423', '9459', '9461', '9465', '9467', '9469',
                '9471', '9473', '9475', '9477', '9479', '9483', '9954', '9955', '9959', '9960', '9961', '9962', '9963',
                '10222', '10225', '10226', '10233', '10235', '10721', '10722', '10725', '10726', '10727', '10732',
                '11340', 'FWS001',
                ]

LAA_Flybait = ['29', '31', '32', '821', '822', '878', '1252', '1255', '1257', ]

# before adjustment to assumptions to exlude false positive in aa
# NLAA_Flybait = ['4', '28', '46', '61', '86', '89', '146', '147', '162', '536', '545', '563', '565', '589', '604', '635',
#                 '684', '688', '697', '720', '755', '772', '776', '781', '801', '814', '833', '851', '895', '915', '938',
#                 '954', '965', '981', '987', '1016', '1049', '1067', '1071', '1083', '1084', '1121', '1135', '1139',
#                 '1146', '1156', '1175', '1182', '1183', '1218', '1224', '1227', '1609', '2144', '2268', '2404', '2619',
#                 '3020', '3175', '3540', '4201', '4551', '6536', '7136', '7948', '8347', '9403', '9459', '9461', '9465',
#                 '9469', '9471', '9473', '9475', '9479', '9483', '9955', '9959', '9961', '9962', '10226', '200', '510',
#                 '535', '537', '572', '575', '577', '590', '601', '616', '622', '623', '687', '690', '691', '725', '726',
#                 '727', '728', '732', '733', '759', '765', '768', '770', '771', '773', '774', '799', '815', '839', '846',
#                 '847', '848', '864', '865', '866', '867', '918', '948', '949', '955', '968', '983', '1051', '1052',
#                 '1054', '1062', '1097', '1099', '1101', '1102', '1104', '1105', '1109', '1112', '1118', '1124', '1128',
#                 '1129', '1144', '1148', '1152', '1155', '1157', '1163', '1176', '1177', '1179', '1180', '1181', '1184',
#                 '1187', '1197', '1201', '1202', '1208', '1211', '1223', '1230', '1232', '1278', '1349', '1361', '1407',
#                 '1521', '1636', '1968', '1989', '2085', '2364', '2517', '2727', '2782', '2970', '3084', '3385', '3387',
#                 '3472', '3653', '3728', '3753', '3876', '4136', '4238', '4297', '4308', '4326', '4377', '4413', '4754',
#                 '4858', '4961', '5333', '5580', '5956', '5991', '6019', '6231', '6303', '6522', '6632', '6654', '6679',
#                 '6747', '6845', '6867', '7170', '7229', '7529', '7840', '7907', '7979', '8338', '8357', '9282', '9395',
#                 '9399', '9405', '9413', '9421', '9423', '9439', '9441', '9467', '9477', '9954', '9958', '9960', '9963',
#                 '10222', '10225', '10227', '10232', '10233', '10235', '10721', '10722', '10725', '10726', '10727',
#                 '10732', '11340', 'FWS001',
#                 ]
# LAA_Flybait = ['29', '31', '32', '821', '822', '878', '1248', '1250', '1252', '1254', '1255', '1256', '1257', '7261',
#                ]

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
# removed species with direct overlap '401','1245', '1246','NMFS166',
NE_DD = ['11191', 'NMFS175', '69', '70', '108', '147', '196', '418', '439', '1361',
         '2144', '4326', '5232', '6231', '6654', '8861', '8962', '76', 'FWS001',
         ]
NLAA_DD = []  # No additional species aew NLAA for DD

NLAA_WoE = [
     '2144', '146', '147', '4136', '6522', '6654', 'FWS001', '162', '3876', '9395', '9403', '9421', '9423',
    '9459', '9461', '9465', '9467', '9469', '9477', '9479', '510', '535', '536', '537', '545', '563', '565', '572',
    '575', '577', '589', '601', '604', '616', '622', '623', '635', '684', '688', '690', '691', '697', '720', '725',
    '726', '727', '728', '732', '733', '755', '765', '768', '771', '772', '773', '774', '776', '781', '801', '814',
    '815', '833', '839', '844', '846', '847', '848', '851', '864', '865', '867', '870', '895', '915', '918', '938',
    '948', '954', '955', '965', '968', '981', '983', '987', '1016', '1049', '1051', '1052', '1054', '1062', '1067',
    '1071', '1083', '1084', '1097', '1099', '1101', '1102', '1104', '1105', '1109', '1112', '1121', '1124', '1128',
    '1129', '1135', '1139', '1144', '1146', '1148', '1152', '1155', '1156', '1157', '1163', '1175', '1176', '1177',
    '1179', '1181', '1182', '1183', '1184', '1197', '1201', '1202', '1208', '1211', '1218', '1223', '1224', '1227',
    '1230', '1232', '1278', '1407', '1521', '1609', '1636', '1968', '2085', '2268', '2404', '2517', '2619', '2727',
    '2782', '2970', '3020', '3084', '3175', '3387', '3472', '3540', '3653', '3728', '3753', '4201', '4297', '4377',
    '4551', '4754', '4858', '4961', '5956', '6019', '6303', '6536', '6632', '6679', '6845', '7136', '7170', '7229',
    '7529', '7840', '7948', '7979', '8181', '8338', '8347', '8357', '9954', '9955', '9959', '9960', '9961', '9962',
    '9963', '10222', '10225', '10226', '10227', '10233', '10235', '10721', '10726', '10727', '11340',

]
collapses_dict = {
    'Bermuda Grass': ['CONUS_Bermuda Grass_0'],
    'Corn': ['AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0', 'CONUS_Corn_0'],
    'Flybait': ['AK_Developed_0', 'AS_Developed_0', 'CNMI_Developed_0', 'CONUS_Developed_0', 'GU_Developed_0',
                'HI_Developed_0', 'PR_Developed_0', 'VI_Developed_0'],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_0', 'HI_Ag_0',
                               'PR_Ag_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Other Crops': ['CONUS_Other Crops_0', 'HI_Ag_0', 'PR_Ag_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0',
                    'GU_Ag_0', 'VI_Ag_0'],
    'Other Grains': ['CONUS_Other Grains_0', 'HI_Ag_0', 'PR_Ag_0', 'AK_Ag_0', 'AS_Ag_0',
                     'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Other RowCrops': ['AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0',
                       'CONUS_Other RowCrops_0'],
    'Pasture': ['AK_Pasture_0', 'HI_Cattle Eartag_0', 'CONUS_Pasture_0', 'AS_Cattle Eartag_0',
                'CNMI_Cattle Eartag_0', 'GU_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0'],
    'Soybeans': ['CONUS_Soybeans_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Vegetables and Ground Fruit': ['CONUS_Vegetables and Ground Fruit_0', 'HI_Ag_0',
                                    'PR_Ag_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'VI_Ag_0'],
    'Methomyl Wheat': ['CONUS_zMethomylWheat_0'],
    'Cotton': ['CONUS_Cotton_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
    'Alley Cropping': ['CONUS_Alley Cropping_0'],
    'Corn_Buffer': ['AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765',
                    'CONUS_Corn_765'],
    'Orchards and Vineyards_Buffer': ['CONUS_Orchards and Vineyards_765', 'HI_Ag_765',
                                      'PR_Ag_765', 'AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765',
                                      'GU_Ag_765', 'VI_Ag_765'],
    'Other Crops_Buffer': ['CONUS_Other Crops_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765',
                           'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'VI_Ag_765'],
    'Other Grains_Buffer': ['CONUS_Other Grains_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765',
                            'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'VI_Ag_765'],
    'Other RowCrops_Buffer': ['AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                              'VI_Ag_765', 'CONUS_Other RowCrops_765'],
    'Pasture_Buffer': ['AK_Pasture_765', 'HI_Cattle Eartag_765', 'CONUS_Pasture_765',
                       'AS_Cattle Eartag_765', 'CNMI_Cattle Eartag_765', 'GU_Cattle Eartag_765', 'PR_Cattle Eartag_765',
                       'VI_Cattle Eartag_765'],
    'Soybeans_Buffer': ['CONUS_Soybeans_765', 'AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                        'PR_Ag_765', 'VI_Ag_765'],
    'Vegetables and Ground Fruit_Buffer': ['CONUS_Vegetables and Ground Fruit_765', 'HI_Ag_765',
                                           'PR_Ag_765', 'AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765',
                                           'GU_Ag_765', 'VI_Ag_765'],
    'Methomyl Wheat_Buffer': ['CONUS_zMethomylWheat_765'],
    'Bermuda Grass_Buffer': ['CONUS_Bermuda Grass_765'],
    'Fly bait_Buffer': ['AK_Developed_765', 'AS_Developed_765', 'CNMI_Developed_765', 'CONUS_Developed_765',
                        'GU_Developed_765',
                        'HI_Developed_765', 'PR_Developed_765', 'VI_Developed_765'],
    'Cotton_Buffer': ['CONUS_Cotton_765', 'AK_Ag_765', 'AS_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                      'PR_Ag_765', 'VI_Ag_765'],
    'Alley Cropping_Buffer': ['CONUS_Alley Cropping_765', ],
    'Methomyl_ActionArea': ['AK_Methomyl_AA_765', 'AS_Methomyl_AA_765', 'CNMI_Methomyl_AA_765', 'CONUS_Methomyl_AA_765',
                            'GU_Methomyl_AA_765', 'HI_Methomyl_AA_765', 'PR_Methomyl_AA_765', 'VI_Methomyl_AA_765'],

}


def step_1_ED(row):
    if row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'May Affect-Extinct'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'May Affect-Outside Use'
    elif row['EntityID'] in NLAA_QualReport:
        return 'May Affect-QualReport'
    elif row['EntityID'] in LAA_QualReport:
        return 'May Affect-QualReport'
    elif row['EntityID'] in NoGIS:
        return 'May Affect-Overlap'
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
    elif row['Methomyl_ActionArea'] == 0:
        return 'NE-Overlap'
    elif row['Methomyl_ActionArea'] > 0:
        return 'May Affect-Overlap'
    else:
        return 'May Affect-Nothing'


def step_2_ED(row):
    if row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'NLAA-Extinct'
    elif str(row['Step 1 ED Comment']).startswith('NE'):
        return 'NE- Step 1'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'NLAA-Outside Use'
    elif row['EntityID'] in NLAA_QualReport:
        return 'NLAA-QualReport'
    elif row['EntityID'] in LAA_QualReport:
        return 'LAA-QualReport'
    elif row['EntityID'] in NoGIS:
        return 'LAA-No GIS'
    elif row['EntityID'] in NLAA_Flybait:
        if row['Fly bait Only'].endswith('Drift Only'):
            if row['Corn_Buffer'] <= 0.44 and row['Orchards and Vineyards_Buffer'] <= 0.44 and row[
                'Other Crops_Buffer'] <= 0.44 \
                    and row['Other Grains_Buffer'] <= 0.44 and row['Other RowCrops_Buffer'] <= 0.44 \
                    and row['Pasture_Buffer'] <= 0.44 and row['Soybeans_Buffer'] <= 0.44 \
                    and row['Vegetables and Ground Fruit_Buffer'] <= 0.44 and row['Methomyl Wheat_Buffer'] <= 0.44 \
                    and row['Bermuda Grass_Buffer'] <= 0.44 and row['Alley Cropping_Buffer'] <= 0.44 \
                    and row['Cotton_Buffer'] <= 0.44:
                if row['EntityID'] in NE_DD:
                    return 'NLAA-Overlap NLAA- Fly bait NE- DD'
                elif row['EntityID'] in DD_Species:
                    return 'LAA- DD NLAA-Overlap NLAA- Fly bait'
                else:
                    return 'NLAA-Overlap NLAA- Fly bait'
            else:
                if row['EntityID'] in NE_DD:
                    return 'LAA-Overlap NLAA- Fly bait NE- DD'
                elif row['EntityID'] in DD_Species:
                    return 'LAA-Overlap LAA- DD NLAA- Fly bait'
                else:
                    return 'LAA-Overlap NLAA- Fly bait'
        else:
            if row['EntityID'] in NE_DD:
                return 'NLAA- Fly bait NE- DD'
            elif row['EntityID'] in DD_Species:
                return 'LAA- DD NLAA- Fly bait'
            else:
                return 'NLAA- Fly bait'
    elif row['Corn_Buffer'] > 0.4 or row['Orchards and Vineyards_Buffer'] > 0.4 or row['Other Crops_Buffer'] > 0.4 \
            or row['Other Grains_Buffer'] > 0.4 or row['Other RowCrops_Buffer'] > 0.4 or row['Pasture_Buffer'] > 0.4 \
            or row['Soybeans_Buffer'] > 0.4 or row['Vegetables and Ground Fruit_Buffer'] > 0.4 \
            or row['Methomyl Wheat_Buffer'] > 0.4 or row['Bermuda Grass_Buffer'] > 0.4 \
            or row['Alley Cropping_Buffer'] > 0.4 or row['Cotton_Buffer'] > 0.4:

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
            and row['Vegetables and Ground Fruit_Buffer'] <= 0.4 and row['Methomyl Wheat_Buffer'] <= 0.4 \
            and row['Bermuda Grass_Buffer'] <= 0.4 and row['Alley Cropping_Buffer'] <= 0.4 \
            and row['Cotton_Buffer'] <= 0.4:
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
        return 'LAA-No Catch'


def flybait_tag(row):
    if row['Flybait'] >= 0:

        if row['Corn'] <= 0.4 and row['Orchards and Vineyards'] <= 0.4 and row['Other Crops'] <= 0.4 and row[
            'Other Grains'] <= 0.4 and row['Other RowCrops'] <= 0.4 and row['Pasture'] <= 0.4 and row[
            'Soybeans'] <= 0.4 and row['Vegetables and Ground Fruit'] <= 0.4 and row['Methomyl Wheat'] <= 0.4 and row[
            'Bermuda Grass'] <= 0.4 and row['Alley Cropping'] <= 0.4 and row['Cotton'] <= 0.4:

            if row['Source of Call final BE-Range'].startswith('Aqua'):
                result = 'No'
            elif row['Step 1 ED Comment'] == 'NE-Overlap':
                result = 'No'
            elif row['Source of Call final BE-Range'].startswith('Qual'):
                result = 'No'
            elif row['EntityID'] in NE_Extinct:
                result = 'No'
            elif row['EntityID'] in NLAA_Extinct:
                result = 'No'
            elif row['EntityID'] in NLAA_OutsideUse:
                result = 'No'
            elif row['EntityID'] in NoGIS:
                result = 'No'
            elif row['Flybait'] == 0:
                result = 'No'
            else:
                result = 'Yes'

            if result == 'Yes':
                if row['Corn_Buffer'] <= 0.4 and row['Orchards and Vineyards_Buffer'] <= 0.4 and row[
                    'Other Crops_Buffer'] <= 0.4 and row['Other Grains_Buffer'] <= 0.4 and row[
                    'Other RowCrops_Buffer'] <= 0.4 and row['Pasture_Buffer'] <= 0.4 and row[
                    'Soybeans_Buffer'] <= 0.4 and row['Vegetables and Ground Fruit_Buffer'] <= 0.4 and row[
                    'Methomyl Wheat_Buffer'] <= 0.4 and row['Bermuda Grass_Buffer'] <= 0.4 and row[
                    'Alley Cropping_Buffer'] <= 0.4 and row['Cotton_Buffer'] <= 0.4:
                    if row['Flybait'] == 0:
                        return 'No'
                    else:
                        return 'Yes'
                else:
                    if row['Flybait'] == 0:
                        return 'No'
                    else:
                        return 'Yes- Spray Drift Only'
            else:
                return 'No'


def drift_tag(row):
    if row['Fly bait Only'] is None or not row['Fly bait Only'].startswith('Yes'):

        if row['Corn'] <= 0.4 and row['Orchards and Vineyards'] <= 0.4 and row['Other Crops'] <= 0.4 and row[
            'Other Grains'] <= 0.4 and row['Other RowCrops'] <= 0.4 and row['Pasture'] <= 0.4 and row[
            'Soybeans'] <= 0.4 and row['Vegetables and Ground Fruit'] <= 0.4 and row['Methomyl Wheat'] <= 0.4 and row[
            'Bermuda Grass'] <= 0.4 and row['Alley Cropping'] <= 0.4 and row['Cotton'] <= 0.4:

            if row['Step 1 ED Comment'] == 'NE-Overlap':
                return 'No'
            elif row['Source of Call final BE-Range'].startswith('Qual'):
                return 'No'
            elif row['EntityID'] in NE_Extinct:
                return 'No'
            elif row['EntityID'] in NLAA_Extinct:
                return 'No'
            elif row['EntityID'] in NLAA_OutsideUse:
                return 'No'
            elif row['EntityID'] in NoGIS:
                return 'No'
            elif row['Corn_Buffer'] > 0.4 or row['Orchards and Vineyards_Buffer'] > 0.4 or row[
                'Other Crops_Buffer'] > 0.4 or row['Other Grains_Buffer'] > 0.4 or row[
                'Other RowCrops_Buffer'] > 0.4 or row['Pasture_Buffer'] > 0.4 or row[
                'Soybeans_Buffer'] > 0.4 or row['Vegetables and Ground Fruit_Buffer'] > 0.4 or row[
                'Methomyl Wheat_Buffer'] > 0.4 or row['Bermuda Grass_Buffer'] > 0.4 or row[
                'Alley Cropping_Buffer'] > 0.4 or row['Cotton_Buffer'] > 0.4:
                return 'Yes'
            else:
                return 'NLAA-overlap'


def apply_ed_flaybait(row, column):
    if row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'NLAA-Extinct'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'NLAA-Outside Use'
    elif row['EntityID'] in LAA_Flybait:
        return 'LAA-Fly bait'
    elif row['EntityID'] in NLAA_Flybait:
        if row['Fly bait Only'].endswith('Drift Only'):
            if row['Corn_Buffer'] <= 0.44 and row['Orchards and Vineyards_Buffer'] <= 0.44 and row[
                'Other Crops_Buffer'] <= 0.44 and row['Other Grains_Buffer'] <= 0.44 and row[
                'Other RowCrops_Buffer'] <= 0.44 and row['Pasture_Buffer'] <= 0.44 and row[
                'Soybeans_Buffer'] <= 0.44 and row['Vegetables and Ground Fruit_Buffer'] <= 0.44 and row[
                'Methomyl Wheat_Buffer'] <= 0.44 and row['Bermuda Grass_Buffer'] <= 0.44 and row[
                'Alley Cropping_Buffer'] <= 0.44 and row['Cotton_Buffer'] <= 0.44:
                if row['EntityID'] in NE_DD:
                    return 'NLAA-Overlap NLAA- Fly bait NE- DD'
                elif row['EntityID'] in DD_Species:
                    return 'LAA- DD NLAA-Overlap NLAA- Fly bait'
                else:
                    return 'NLAA-Overlap NLAA- Fly bait'

            else:
                if row['EntityID'] in NE_DD:
                    return 'LAA-Overlap NLAA- Fly bait NE- DD'
                elif row['EntityID'] in DD_Species:
                    return 'LAA-Overlap LAA- DD NLAA- Fly bait'
                else:
                    return 'LAA-Overlap NLAA- Fly bait'
        else:
            if row['EntityID'] in NE_DD:
                return 'NLAA- Fly bait NE- DD'
            elif row['EntityID'] in DD_Species:
                return 'LAA- DD NLAA- Fly bait'
            else:
                return 'NLAA- Fly bait'


    else:
        value = row[column]
        return value


def clean_up_columns(row, column):
    try:

        value = str(row[column].split('-')[0])

        return value
    except:
        return 'check'


def apply_ed_woe(row, column):
    value = row[column]
    if row['EntityID'] in NE_Extinct:
        return 'NE-Extinct'
    elif row['EntityID'] in NLAA_Extinct:
        return 'NLAA-Extinct'
    elif row['EntityID'] in NLAA_OutsideUse:
        return 'NLAA-Outside Use'
    elif row['EntityID'] in LAA_Flybait:
        return 'LAA-Fly bait'
    elif row['EntityID'] in NLAA_QualReport:
        return 'NLAA-QualReport'
    elif row['EntityID'] in LAA_QualReport:
        return 'LAA-QualReport'
    elif row['EntityID'] in NoGIS:
        return 'LAA-No GIS'
    elif value == 'NE- Step 1':
        return value
    elif value == 'NLAA-Overlap':
        return value
    elif row['EntityID'] in NLAA_Flybait:
        if row['Fly bait Only'].endswith('Drift Only'):
            if row['Corn_Buffer'] <= 0.44 and row['Orchards and Vineyards_Buffer'] <= 0.44 and row[
                'Other Crops_Buffer'] <= 0.44 and row['Other Grains_Buffer'] <= 0.44 and row[
                'Other RowCrops_Buffer'] <= 0.44 and row['Pasture_Buffer'] <= 0.44 and row[
                'Soybeans_Buffer'] <= 0.44 and row['Vegetables and Ground Fruit_Buffer'] <= 0.44 and row[
                'Methomyl Wheat_Buffer'] <= 0.44 and row['Bermuda Grass_Buffer'] <= 0.44 and row[
                'Alley Cropping_Buffer'] <= 0.44 and row['Cotton_Buffer'] <= 0.44:
                if row['EntityID'] in NE_DD:
                    return 'NLAA-Overlap NLAA- Fly bait NE- DD'
                elif row['EntityID'] in DD_Species:
                    return 'LAA- DD NLAA-Overlap NLAA- Fly bait'
                else:
                    return 'NLAA-Overlap NLAA- Fly bait'
            else:
                if row['EntityID'] in NE_DD:
                    if row['EntityID'] in NLAA_WoE:
                        return 'NLAA- WoE LAA-Overlap NLAA- Fly bait NE- DD'
                    else:
                        return 'LAA- WoE LAA-Overlap NLAA- Fly bait NE- DD'
                elif row['EntityID'] in DD_Species:
                    if row['EntityID'] in NLAA_WoE:
                        return 'NLAA- WoE LAA-Overlap LAA- DD NLAA- Fly bait'
                    else:
                        return 'LAA- WoE LAA-Overlap LAA- DD NLAA- Fly bait'
                else:
                    if row['EntityID'] in NLAA_WoE:
                        return 'NLAA- WoE LAA-Overlap NLAA- Fly bait'
                    else:
                        return 'LAA- WoE LAA-Overlap NLAA- Fly bait'
        else:
            if row['EntityID'] in NE_DD:
                if row['EntityID'] in NLAA_WoE:
                    return 'NLAA-WoE NLAA- Fly bait NE- DD'
                else:
                    return 'NLAA- Fly bait NE- DD'
            elif row['EntityID'] in DD_Species:
                if row['EntityID'] in NLAA_WoE:
                    return 'NLAA-WoE LAA- DD NLAA- Fly bait'
                else:
                    return 'LAA- DD NLAA- Fly bait'
            else:
                if row['EntityID'] in NLAA_WoE:
                    return 'NLAA-WoE NLAA- Fly bait'
                else:
                    return 'NLAA- Fly bait'
    elif row['Corn_Buffer'] >= 0.44 or row['Orchards and Vineyards_Buffer'] >= 0.44 or row[
        'Other Crops_Buffer'] >= 0.44 or row['Other Grains_Buffer'] >= 0.44 or row[
        'Other RowCrops_Buffer'] >= 0.44 or row['Pasture_Buffer'] >= 0.44 or row[
        'Soybeans_Buffer'] >= 0.44 or row['Vegetables and Ground Fruit_Buffer'] >= 0.44 or row[
        'Methomyl Wheat_Buffer'] >= 0.44 or row['Bermuda Grass_Buffer'] >= 0.44 or row[
        'Alley Cropping_Buffer'] >= 0.44 or row['Cotton_Buffer'] >= 0.44:
        if row['EntityID'] in NLAA_WoE:
            if row['EntityID'] in NE_DD:
                return 'NLAA-WoE LAA- Overlap NE-DD'
            elif row['EntityID'] in DD_Species:
                return 'NLAA-WoE LAA- Overlap LAA-DD'
            else:

                return 'NLAA-WoE LAA- Overlap'
        else:
            if row['EntityID'] in NE_DD:
                return 'LAA-WoE LAA- Overlap NE-DD'
            elif row['EntityID'] in DD_Species:
                return 'LAA-WoE LAA- Overlap LAA-DD'
            else:
                return 'LAA-WoE LAA- Overlap'
    elif row['EntityID'] in NLAA_WoE:
        if row['EntityID'] in NE_DD:
            return 'NLAA-WoE LAA- Overlap NE-DD'
        elif row['EntityID'] in DD_Species:
            return 'NLAA-WoE LAA- Overlap LAA-DD'
        else:
            return 'NLAA-WoE LAA- Overlap'
    elif row['EntityID'] in NE_DD:
        return 'LAA-WoE LAA- Overlap NE-DD'
    elif row['EntityID'] in DD_Species:
        return 'LAA-WoE LAA- Overlap LAA-DD'
    else:
        return 'LAA-WoE LAA- Overlap'


def summary_table(step1, step2, groups):
    df_summary = pd.DataFrame(data=groups, columns=['WoE Summary Group'])
    df_summary = df_summary.reindex(columns=['WoE Summary Group', 'NE', 'May Affect', 'NLAA', 'LAA', 'TBD'])

    for group in groups:
        for cols in ['NE', 'May Affect']:
            group_df = step1.loc[step1['WoE Summary Group'] == str(group)]
            try:
                value = group_df.loc[group_df['Step 1 ED'] == cols].iloc[0, 2]
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = value
            except:
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = 0
        for cols in ['NLAA', 'LAA', 'TBD']:
            group_df = step2.loc[step2['WoE Summary Group'] == str(group)]
            try:
                value = group_df.loc[group_df['Step 2 ED'] == cols].iloc[0, 2]
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = value
            except:
                df_summary.loc[df_summary['WoE Summary Group'] == group, cols] = 0
    df_summary['Total'] = df_summary['NE'] + df_summary['May Affect']
    df_summary = df_summary.append(df_summary.sum(numeric_only=True, axis=0), ignore_index=True)
    print df_summary
    return df_summary


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_uses = collapses_dict.keys()
print list_uses
# Sets up the intervals that are of interests for each of the uses

sp_table_df = pd.read_csv(in_table, dtype=object)
print sp_table_df.columns.values.tolist()
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
# ### Step 1

collapsed_df['Step 1 ED Comment'] = collapsed_df.apply(lambda row: step_1_ED(row), axis=1)

collapsed_df['Step 1 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 1 ED Comment'), axis=1)

collapsed_df['Fly bait Only'] = collapsed_df.apply(lambda row: flybait_tag(row), axis=1)
collapsed_df['Drift Only-NonFly bait'] = collapsed_df.apply(lambda row: drift_tag(row), axis=1)
collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: step_2_ED(row), axis=1)

collapsed_df['Step 1 ED'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'Step 1 ED'), axis=1)
collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: apply_ed_flaybait(row, 'Step 2 ED Comment'),
                                                       axis=1)

collapsed_df['Step 2 ED Comment'] = collapsed_df.apply(lambda row: apply_ed_woe(row, 'Step 2 ED Comment'),
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
