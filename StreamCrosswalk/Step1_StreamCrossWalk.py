import os
import gc
import time
import datetime
import csv
import functions
import arcpy

gc.enable()

# possibly speed by loading data into arrays?
# write to a CSV at end for each rather than using table to table conversion
# add code to export what is "print" to a txt so that the selection numbers can be Qaed

# User variable
# Workspace

# NOTE NOTE if the script is stop and a table was start but not completed for a species it  must be deleted before
# Starting the script again.  If a table has been created the script will move to the next species
ws = "L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\CriticalHabitat"
#ws = "L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\Range"
name_dir = 'AllSp_MasterList_20161124'

# GDB with all Composites to Run
# MasterSpeFC = r"L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb"
MasterSpeFC = r"L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb"
# File type to flag the species with
path, gdb = os.path.split(MasterSpeFC)
file_type = gdb.split('_')[0]
# out csv for species without any streams
noNHDCSV = "FWS_" + str(file_type) + "_l48_noNHD2_20162411"

# name of field that will ID the HUC 2 and location of that file, first all of the HUC2 are identifies so that the
# correct folder from the NHD can be checked
HUC2Field = "VPU_ID"
HUC2_lwr48 = "L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Boundaries.gdb\VPU_Final"

# Stream files, the HUC 2 number needs to match the ID field above
huc1 = "L:\NHDPlusV2\NHDPlus01\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc2 = "L:\NHDPlusV2\NHDPlus02\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3N = "L:\NHDPlusV2\NHDPlus03N\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3S = "L:\NHDPlusV2\NHDPlus03S\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3W = "L:\NHDPlusV2\NHDPlus03W\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc4 = "L:\NHDPlusV2\NHDPlus04\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc5 = "L:\NHDPlusV2\NHDPlus05\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc6 = "L:\NHDPlusV2\NHDPlus06\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc7 = "L:\NHDPlusV2\NHDPlus07\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc8 = "L:\NHDPlusV2\NHDPlus08\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc9 = "L:\NHDPlusV2\NHDPlus09\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc10L = "L:\NHDPlusV2\NHDPlus10L\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc10U = "L:\NHDPlusV2\NHDPlus10U\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc11 = "L:\NHDPlusV2\NHDPlus11\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc12 = "L:\NHDPlusV2\NHDPlus12\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc13 = "L:\NHDPlusV2\NHDPlus13\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc14 = "L:\NHDPlusV2\NHDPlus14\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc15 = "L:\NHDPlusV2\NHDPlus15\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc16 = "L:\NHDPlusV2\NHDPlus16\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc17 = "L:\NHDPlusV2\NHDPlus17\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc18 = "L:\NHDPlusV2\NHDPlus18\NHDSnapshot\Hydrography\NHDFlowline.shp"
# List of species to consider --DD species
species_to_run = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '11', '12', '13', '14', '16', '17', '19', '20', '21',
                  '23', '24', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '37', '38', '39', '40', '41',
                  '42', '43', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59',
                  '60', '61', '62', '64', '65', '66', '68', '69', '70', '71', '72', '73', '74', '75', '78', '79', '80',
                  '81', '82', '83', '84', '85', '86', '87', '88', '93', '94', '95', '96', '97', '98', '100', '101',
                  '102', '103', '105', '106', '107', '109', '110', '111', '112', '113', '114', '115', '116', '117',
                  '118', '119', '120', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132',
                  '133', '134', '135', '136', '137', '138', '139', '141', '142', '143', '145', '146', '147', '148',
                  '149', '150', '151', '152', '153', '154', '155', '156', '160', '162', '163', '164', '165', '166',
                  '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180',
                  '181', '182', '183', '185', '187', '188', '189', '191', '192', '193', '194', '195', '196', '197',
                  '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211',
                  '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225',
                  '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239',
                  '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253',
                  '254', '255', '256', '257', '258', '259', '260', '261', '262', '263', '264', '265', '266', '267',
                  '268', '269', '270', '271', '272', '273', '274', '275', '276', '277', '278', '279', '280', '281',
                  '282', '283', '284', '285', '286', '287', '288', '290', '291', '292', '293', '294', '295', '296',
                  '297', '298', '299', '300', '301', '303', '305', '306', '307', '308', '309', '311', '312', '313',
                  '314', '315', '316', '317', '318', '319', '320', '321', '322', '323', '324', '325', '326', '327',
                  '328', '329', '330', '331', '332', '333', '334', '335', '336', '337', '338', '339', '340', '341',
                  '342', '343', '344', '345', '346', '347', '348', '349', '350', '351', '352', '353', '354', '355',
                  '356', '357', '358', '359', '360', '361', '362', '363', '364', '365', '366', '367', '368', '369',
                  '370', '371', '372', '373', '374', '375', '376', '377', '378', '379', '380', '381', '382', '383',
                  '384', '385', '386', '387', '389', '390', '391', '392', '393', '394', '395', '396', '398', '399',
                  '400', '401', '402', '403', '404', '406', '407', '408', '409', '411', '412', '413', '414', '415',
                  '416', '417', '418', '419', '420', '421', '422', '423', '424', '425', '426', '427', '428', '429',
                  '430', '431', '432', '433', '434', '435', '436', '437', '438', '439', '440', '441', '442', '443',
                  '444', '445', '446', '447', '448', '449', '450', '451', '452', '453', '454', '455', '456', '457',
                  '458', '459', '460', '461', '462', '463', '464', '465', '466', '467', '468', '469', '470', '471',
                  '472', '473', '474', '475', '476', '477', '478', '479', '480', '481', '482', '483', '484', '485',
                  '486', '487', '488', '489', '490', '491', '492', '493', '494', '495', '496', '497', '498', '499',
                  '500', '501', '502', '503', '504', '505', '506', '507', '508', '509', '510', '511', '512', '513',
                  '514', '515', '516', '517', '518', '519', '520', '521', '522', '523', '524', '525', '526', '527',
                  '528', '529', '530', '531', '532', '533', '534', '535', '536', '537', '538', '539', '540', '541',
                  '542', '543', '544', '545', '546', '547', '548', '549', '550', '551', '552', '553', '554', '555',
                  '556', '557', '558', '559', '560', '561', '562', '563', '564', '565', '566', '567', '568', '569',
                  '570', '571', '572', '573', '574', '575', '576', '577', '578', '579', '580', '581', '582', '583',
                  '584', '585', '586', '587', '588', '589', '590', '591', '592', '593', '594', '595', '596', '597',
                  '598', '599', '600', '601', '602', '603', '604', '605', '606', '607', '608', '609', '610', '611',
                  '612', '613', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '624', '625',
                  '626', '627', '628', '629', '630', '631', '632', '633', '634', '635', '636', '637', '638', '639',
                  '640', '641', '642', '643', '644', '645', '646', '647', '648', '649', '650', '651', '652', '653',
                  '654', '655', '656', '657', '658', '659', '660', '661', '662', '663', '664', '665', '666', '667',
                  '668', '669', '670', '671', '672', '673', '674', '675', '676', '677', '678', '679', '680', '681',
                  '682', '683', '684', '685', '686', '687', '688', '689', '690', '691', '692', '693', '694', '695',
                  '696', '697', '698', '700', '701', '702', '703', '704', '705', '707', '708', '709', '710', '711',
                  '712', '713', '715', '716', '717', '718', '719', '720', '721', '722', '723', '724', '725', '726',
                  '727', '728', '729', '730', '731', '732', '733', '734', '735', '736', '737', '738', '739', '740',
                  '741', '742', '743', '744', '745', '746', '747', '748', '749', '750', '751', '752', '753', '754',
                  '755', '756', '757', '758', '759', '760', '761', '762', '763', '764', '765', '766', '767', '768',
                  '769', '770', '771', '772', '773', '774', '775', '776', '777', '778', '779', '780', '781', '782',
                  '783', '784', '785', '786', '787', '788', '789', '790', '791', '792', '793', '794', '795', '796',
                  '797', '798', '799', '800', '801', '802', '803', '804', '805', '806', '807', '808', '809', '810',
                  '811', '812', '813', '814', '815', '816', '817', '818', '819', '820', '821', '822', '823', '824',
                  '825', '826', '827', '828', '829', '830', '831', '832', '833', '834', '835', '836', '837', '838',
                  '839', '840', '841', '842', '843', '844', '845', '846', '847', '848', '849', '850', '851', '852',
                  '853', '854', '855', '856', '857', '858', '859', '860', '861', '862', '863', '864', '865', '866',
                  '867', '868', '869', '870', '871', '872', '873', '874', '875', '876', '878', '879', '880', '881',
                  '882', '883', '884', '885', '886', '887', '888', '889', '890', '891', '892', '893', '894', '895',
                  '896', '897', '898', '899', '900', '901', '902', '903', '904', '905', '906', '907', '908', '909',
                  '910', '911', '912', '913', '914', '915', '916', '917', '918', '919', '920', '921', '922', '923',
                  '924', '925', '926', '927', '928', '929', '930', '931', '932', '933', '934', '935', '936', '937',
                  '938', '939', '940', '941', '942', '943', '945', '946', '947', '948', '949', '950', '951', '952',
                  '953', '954', '955', '956', '957', '958', '959', '960', '961', '962', '963', '964', '965', '966',
                  '967', '968', '969', '970', '971', '972', '973', '974', '975', '976', '977', '978', '979', '980',
                  '981', '982', '983', '984', '985', '986', '987', '988', '989', '990', '991', '992', '993', '994',
                  '995', '996', '997', '998', '999', '1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007',
                  '1008', '1009', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', '1018', '1019',
                  '1020', '1021', '1022', '1023', '1024', '1025', '1026', '1027', '1028', '1029', '1030', '1031',
                  '1032', '1033', '1034', '1035', '1036', '1037', '1038', '1039', '1040', '1041', '1042', '1043',
                  '1044', '1045', '1046', '1047', '1048', '1049', '1050', '1051', '1052', '1053', '1054', '1055',
                  '1056', '1057', '1058', '1059', '1060', '1061', '1062', '1063', '1064', '1065', '1066', '1067',
                  '1068', '1069', '1070', '1071', '1072', '1073', '1074', '1075', '1076', '1077', '1078', '1079',
                  '1080', '1081', '1082', '1083', '1084', '1085', '1086', '1087', '1088', '1089', '1090', '1091',
                  '1092', '1093', '1094', '1095', '1096', '1097', '1098', '1099', '1100', '1101', '1102', '1103',
                  '1104', '1105', '1106', '1107', '1108', '1109', '1110', '1111', '1112', '1113', '1114', '1115',
                  '1116', '1117', '1118', '1119', '1120', '1121', '1122', '1123', '1124', '1125', '1126', '1127',
                  '1128', '1129', '1130', '1131', '1132', '1133', '1134', '1135', '1136', '1137', '1138', '1139',
                  '1140', '1141', '1142', '1143', '1144', '1145', '1146', '1147', '1148', '1149', '1150', '1151',
                  '1152', '1153', '1154', '1155', '1156', '1157', '1158', '1159', '1160', '1162', '1163', '1164',
                  '1165', '1166', '1167', '1168', '1169', '1170', '1171', '1172', '1173', '1174', '1175', '1176',
                  '1177', '1178', '1179', '1180', '1181', '1182', '1183', '1184', '1185', '1186', '1187', '1188',
                  '1189', '1190', '1191', '1192', '1193', '1194', '1195', '1196', '1197', '1198', '1199', '1200',
                  '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1210', '1211', '1212',
                  '1213', '1214', '1215', '1216', '1217', '1218', '1219', '1220', '1221', '1222', '1223', '1224',
                  '1225', '1226', '1227', '1228', '1229', '1230', '1231', '1232', '1233', '1234', '1235', '1236',
                  '1237', '1238', '1239', '1240', '1241', '1245', '1246', '1247', '1248', '1249', '1250', '1251',
                  '1252', '1253', '1254', '1255', '1256', '1257', '1258', '1259', '1261', '1262', '1263', '1264',
                  '1265', '1266', '1267', '1278', '1302', '1311', '1349', '1358', '1361', '1369', '1378', '1380',
                  '1400', '1407', '1415', '1450', '1497', '1502', '1509', '1521', '1525', '1535', '1559', '1607',
                  '1609', '1623', '1632', '1636', '1645', '1678', '1680', '1693', '1707', '1709', '1710', '1737',
                  '1740', '1760', '1769', '1783', '1831', '1840', '1849', '1862', '1881', '1897', '1905', '1934',
                  '1953', '1968', '1984', '1989', '2036', '2085', '2118', '2142', '2144', '2154', '2192', '2211',
                  '2265', '2268', '2273', '2278', '2308', '2316', '2364', '2389', '2404', '2448', '2458', '2510',
                  '2514', '2517', '2528', '2561', '2567', '2599', '2619', '2682', '2683', '2727', '2730', '2758',
                  '2767', '2778', '2780', '2782', '2810', '2823', '2842', '2859', '2860', '2862', '2891', '2917',
                  '2929', '2934', '2956', '2970', '3020', '3049', '5153', '3084', '3096', '3116', '3133', '3154',
                  '3175', '3194', '3199', '3224', '3226', '3267', '3271', '3280', '3292', '3318', '3364', '3379',
                  '3385', '3387', '3388', '3398', '3412', '3472', '3497', '3525', '3532', '3540', '3592', '3596',
                  '3628', '3645', '3653', '3654', '3670', '3671', '3722', '3728', '3737', '3753', '3784', '3832',
                  '3833', '3842', '3849', '3871', '3876', '3879', '3990', '3999', '4000', '4007', '4030', '4042',
                  '4064', '4086', '4090', '4093', '4112', '4136', '4162', '4179', '4201', '4210', '4228', '4237',
                  '4238', '4248', '4253', '4274', '4296', '4297', '4300', '4308', '4326', '4330', '4369', '4377',
                  '4395', '4411', '4413', '4420', '4431', '4437', '4479', '4487', '4490', '4496', '4508', '4533',
                  '4551', '4564', '4565', '4589', '4630', '4679', '4680', '4712', '4719', '4724', '4740', '4754',
                  '4766', '4773', '4799', '4858', '4881', '4889', '4910', '4961', '4992', '5064', '5065', '5067',
                  '5104', '7372', '5168', '5170', '5180', '5186', '5210', '5212', '5232', '5233', '5265', '5273',
                  '5281', '5333', '5334', '5362', '5434', '5449', '5468', '5580', '11201', '5623', '5658', '5709',
                  '5714', '5715', '5718', '5719', '5763', '5815', '5833', '5856', '5956', '5981', '5989', '5991',
                  '6019', '6062', '6097', '6138', '6176', '6220', '6223', '6231', '6257', '6297', '6345', '6346',
                  '6490', '6503', '6522', '6534', '6536', '6557', '6578', '6596', '6617', '6618', '6620', '6632',
                  '6654', '6662', '6672', '6679', '6739', '6747', '6782', '6841', '6843', '6845', '6867', '6870',
                  '6901', '6966', '6969', '7046', '7054', '7067', '7091', '7115', '7116', '7134', '7136', '7150',
                  '7167', '7170', '7177', '7206', '7220', '7229', '7254', '7261', '7264', '7280', '7332', '7342',
                  '7349', '7363', '7367', '11262', '7482', '7495', '7512', '7529', '7572', '7590', '7610', '7617',
                  '7670', '7731', '7800', '7805', '7816', '7834', '7840', '7847', '7855', '7886', '7892', '7907',
                  '7918', '7948', '7949', '7955', '7979', '7989', '7992', '8083', '8166', '8172', '8181', '8231',
                  '8241', '8254', '8277', '8278', '8303', '8336', '8338', '8347', '8349', '8356', '8357', '8386',
                  '8389', '8392', '8395', '8434', '8442', '8462', '8503', '8561', '8621', '8683', '8684', '8685',
                  '8765', '8861', '8921', '8962', '9001', '9021', '9061', '9122', '9126', '9220', '9282', '9338',
                  '9378', '9382', '9384', '9395', '9397', '9399', '9401', '9403', '9405', '9407', '9409', '9411',
                  '9413', '9415', '9417', '9419', '9421', '9423', '9437', '9439', '9441', '9447', '9451', '9455',
                  '9457', '9459', '9463', '9465', '9467', '9469', '9471', '9473', '9475', '9477', '9481', '9483',
                  '9487', '9488', '9489', '9490', '9491', '9492', '9493', '9494', '9495', '9496', '9497', '9498',
                  '9499', '9500', '9501', '9502', '9503', '9504', '9505', '9506', '9507', '9694', '9707', '9709',
                  '9721', '9725', '9929', '9941', '9943', '9951', '9952', '9953', '9954', '9955', '9956', '9957',
                  '9958', '9959', '9960', '9961', '9962', '9963', '9965', '9967', '9968', '9969', '10007', '10008',
                  '10009', '10010', '10013', '10021', '10028', '10034', '10037', '10038', '10039', '10043', '10052',
                  '10060', '10076', '10077', '10124', '10130', '10141', '10142', '10144', '10145', '10147', '10150',
                  '10151', '10153', '10161', '10178', '10222', '10223', '10224', '10225', '10226', '10227', '10228',
                  '10229', '10230', '10231', '10232', '10233', '10234', '10235', '10290', '10297', '10298', '10299',
                  '10300', '10301', '10310', '10311', '10312', '10314', '10319', '10323', '10326', '10332', '10340',
                  '10341', '10370', '10381', '10479', '10480', '10481', '10483', '10484', '10485', '10582', '10583',
                  '10584', '10585', '10586', '10587', '10588', '10590', '10591', '10592', '10593', '10594', '10599',
                  '10700', 'NMFS173', 'NMFS174', '3054', '5610', '10719', '10720', '10721', '10722', '10723', '10724',
                  '10725', '10726', '10733', '10734', '10736', '10903', '10908', '10727', '10910', '11175', '11176',
                  '11191', '11192', '11193', '10728', '10729', '10732', '10909', '11260', '11333', 'NMFS125', 'NMFS134',
                  'NMFS137', 'NMFS139', 'NMFS159', 'NMFS166', '11340', 'FWS001', 'NMFS175', 'NMFS178', 'NMFS180'

                  ]



# timers


def start_times(start_clock):
    start_time = datetime.datetime.fromtimestamp(start_clock)
    print "Start Time: " + str(start_time)
    print start_time.ctime()


def end_times(end_clock, start_clock):
    start_time = datetime.datetime.fromtimestamp(start_clock)
    end = datetime.datetime.fromtimestamp(end_clock)
    print "End Time: " + str(end)
    print end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)


# creates directories
def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        print "created directory {0}".format(dbf_dir)


def stream_crosswalk(in_fc, HUC2_Lowr48, LYR_dir, DBF_dir, HUCdict, NoNHD, csvpath):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    # each row is a species
    # for each species first the HUC 2s are identified then the stream in each HUC are select and export to one master
    # table for the species
    # output is all streams for a species across all HUC2
    for row in arcpy.SearchCursor(in_fc):
        lyr = file_type + "_{0}_lyr".format(row.EntityID)  # layer name for selected features
        out_layer = LYR_dir + os.sep + lyr + ".lyr"
        table = "{1}_Streams_{0}.dbf".format(row.EntityID, file_type)
        temptable = os.path.join(DBF_dir, table)
        if arcpy.Exists(temptable):
            continue

        # NOTE NOTE HARD CODE TO ENTITYID
        entid = "{0}".format(row.EntityID)  # Extract entity ID number
        if entid not in species_to_run:  # skips species not in list
            continue
        if not arcpy.Exists(out_layer):
            whereclause = "EntityID = '%s'" % entid
            print whereclause
            # Makes a feature layer that will only include current entid using whereclause
            arcpy.MakeFeatureLayer_management(in_fc, lyr, whereclause)
            print "Creating layer {0}".format(lyr)
            arcpy.SaveToLayerFile_management(lyr, out_layer, "ABSOLUTE")
            print "Saved layer file"
        if not arcpy.Exists(temptable):
            spec_location = str(out_layer)
            # check for HUC a species occurs in

            arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
            arcpy.Delete_management("slt_lyr")
            arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
            # saves all HUC2 to a list this will have duplicates they are removed using the set below
            with arcpy.da.SearchCursor("slt_lyr", HUC2Field) as cursor:
                HUC2list = sorted({row[0] for row in cursor})

            print HUC2list

            # for each value in the HUC2 set will select all stream that are with species file, and save to a master
            # species table, one table per species will include all values
            counter = 0
            for z in HUC2list:
                print z
                huc_fc = HUCdict[z]
                print huc_fc

                arcpy.Delete_management("huc_lyr")
                arcpy.MakeFeatureLayer_management(huc_fc, "huc_lyr")

                # NOTE NOTE would a different selection type be better or should multople be used?
                arcpy.SelectLayerByLocation_management("huc_lyr", "INTERSECT", out_layer, "", "NEW_SELECTION")
                count = arcpy.GetCount_management("huc_lyr")
                print str(count) + " selected features"
                tableview = "tbl_view_" + str(counter)
                arcpy.Delete_management(tableview)
                arcpy.MakeTableView_management("huc_lyr", tableview)
                count = int(arcpy.GetCount_management(tableview).getOutput(0))
                print str(count) + " Tableview"
                if count < 1:
                    print 'Zero'
                    filename = str(lyr)
                    if filename not in NoNHD:
                        NoNHD.append(filename)
                    continue

                    # NOTE NOTE if the script is stop and a table was start but not completed for a species it
                    # must be deleted before starting the script again.If a table has been created the script will
                    # move to the next species
                if counter == 0:  # This will be for first HUC (counter =0) for the species the table is  created
                    if count != 0:
                        filename = str(lyr)
                        arcpy.TableToTable_conversion(tableview, DBF_dir, table)
                        print "created table: " + str(temptable)
                        counter += 1
                        if filename in NoNHD:
                            NoNHD.remove(filename)
                else:  # remaining results for additional HUC selection values will be appened to table
                    arcpy.Append_management(tableview, temptable, "NO_TEST", "", "")
                    counter += 1
            print "table {0} completed. Located at {1}".format(temptable, DBF_dir)

        else:
            print "Stream Crosswalk for {0}".format(lyr) + " previously populated"

    arcpy.Delete_management("HUC48_lyr")

    create_outtable(NoNHD, csvpath)
    return NoNHD


# exports list to csv
def create_outtable(list_name, csv_location):
    with open(csv_location, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


# Creates CSV names with timestamp
def create_csvflnm_timestamp(name_csv_file, out_csv_location):
    filename = str(name_csv_file) + "_" + str(date_list[0]) + '.csv'
    filepath = os.path.join(out_csv_location, filename)
    return filename, filepath


date_list = []
today = datetime.date.today()
date_list.append(today)

# workspace and output paths
File_dir = ws + os.sep + str(name_dir)
LYR_dir = File_dir + os.sep + 'LYR'
DBF_dir = File_dir + os.sep + 'DBF'
csvfile, csvpath = create_csvflnm_timestamp(noNHDCSV, DBF_dir)
noNHD = []  # empty list to hold species without any streams; this is exported to a csv at end

# Set up HUC DICT
HUCdict = {}
for k, v in globals().items():
    if k.startswith('huc'):
        num = k.replace("huc", "")
        HUCdict[num] = v

start = time.time()
start_times(start)

# Create Workspaces
create_directory(File_dir)
create_directory(LYR_dir)
create_directory(DBF_dir)

# Execute loop that will check each input comp fc to see if a species in the species list is present, and if so it will
# extract all streams with in the range and save to a dbf
for fc in functions.fcs_in_workspace(MasterSpeFC):
    check_prj = fc.split('_')
    try:
        if 'AlbersUSA' == check_prj[4]:
            continue
    except:
        pass

    print fc
    input_fc = MasterSpeFC + os.sep + fc
    stream_crosswalk(input_fc, HUC2_lwr48, LYR_dir, DBF_dir, HUCdict, noNHD, csvpath)

print noNHD
done = time.time()
end_times(done, start)
