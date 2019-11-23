import pandas as pd

import datetime
import os

# TODO remove function for hab and elevation and just send the working elevation and habitat df through the habitat function
# todo categories might not work for elevation
# TODO ADD BACK IN ELEVATION
#TODO set up a try except to delete partial files if the script bombs

# This is on run on Range right now we do not adjust CH by hab anf elevation - long term merge scripts so one can be
# used


# LOOKUP TABLE FROM UNIONS and OTHER INPUT FILES
in_directory_grids = r'E:\Workspace\StreamLine\ESA\UnionFiles_Summer2019\Range\SpComp_UsageHUCAB_byProjection\Grid_byProjections_Combined'
look_up_fc = r'E:\Workspace\StreamLine\ESA\UnionFiles_Summer2019\Range\LookUp_Grid_byProjections_Combined'

in_directory_csv = r'E:\Workspace\StreamLine\ESA\Results_HabEle\L48\Range\Agg_Layers'
out_path = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat'
out_poltical = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\PolBoundaries'
#
# in_directory_csv = r'E:\Workspace\StreamLine\ESA\Results_HabEle\NL48\Range\Agg_Layers'
# out_path = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat'
# out_poltical = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\PolBoundaries'

skip_regions = ['AS', 'CNMI','GU', 'PR', 'VI']
# 'r_birds', 'r_clams', 'r_flower', 'r_mammal', 'r_conife', 'r_amphib', 'r_crusta', 'r_fishes', 'r_reptil']
skip_species = []

folder_skip = ['CONUS_carbaryl_AA_190301_dev_euc', 'CONUS_carbaryl_AA_190507_dev_euc',
               'CONUS_carbaryl_AA_ag_190301_AA_euc', 'CONUS_carbaryl_AA_ag_190507_dev_euc',
               'CONUS_carbaryl_AA_nonAg_190301_AA_euc', 'CONUS_carbaryl_AA_nonAg_190507_dev_euc',
               'CONUS_carbaryl_AA_woDev_190301_AA_euc','CONUS_methomyl_AA_190301_dev_euc',
               'CONUS_methomyl_AA_190507_dev_euc', 'CONUS_methomyl_AA_Ag_190301_dev_euc',
               'CONUS_methomyl_AA_Ag_190507_dev_euc','CONUS_carbaryl_AA_Ag_191106_euc', 'CONUS_carbaryl_AA_191106_euc',
               'CONUS_carbaryl_AA_NonAg_191106_euc', ]  # name of use folder with results to skip over

final_col_order = ['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', 'VALUE_0', 'VALUE_30', 'VALUE_42', 'VALUE_60',
                   'VALUE_67', 'VALUE_84', 'VALUE_90', 'VALUE_94', 'VALUE_108', 'VALUE_120', 'VALUE_123', 'VALUE_127',
                   'VALUE_134', 'VALUE_150', 'VALUE_152', 'VALUE_161', 'VALUE_169', 'VALUE_174', 'VALUE_180',
                   'VALUE_182', 'VALUE_189', 'VALUE_192', 'VALUE_201', 'VALUE_210', 'VALUE_212', 'VALUE_216',
                   'VALUE_218', 'VALUE_228', 'VALUE_234', 'VALUE_240', 'VALUE_241', 'VALUE_247', 'VALUE_254',
                   'VALUE_256', 'VALUE_258', 'VALUE_268', 'VALUE_270', 'VALUE_271', 'VALUE_276', 'VALUE_283',
                   'VALUE_284', 'VALUE_295', 'VALUE_296', 'VALUE_300', 'VALUE_301', 'VALUE_305', 'VALUE_308',
                   'VALUE_313', 'VALUE_318', 'VALUE_323', 'VALUE_324', 'VALUE_330', 'VALUE_331', 'VALUE_335',
                   'VALUE_339', 'VALUE_342', 'VALUE_349', 'VALUE_351', 'VALUE_360', 'VALUE_361', 'VALUE_362',
                   'VALUE_364', 'VALUE_366', 'VALUE_371', 'VALUE_375', 'VALUE_379', 'VALUE_381', 'VALUE_384',
                   'VALUE_390', 'VALUE_391', 'VALUE_394', 'VALUE_400', 'VALUE_402', 'VALUE_403', 'VALUE_408',
                   'VALUE_416', 'VALUE_417', 'VALUE_420', 'VALUE_421', 'VALUE_424', 'VALUE_426', 'VALUE_429',
                   'VALUE_432', 'VALUE_436', 'VALUE_442', 'VALUE_445', 'VALUE_450', 'VALUE_453', 'VALUE_456',
                   'VALUE_457', 'VALUE_458', 'VALUE_465', 'VALUE_466', 'VALUE_468', 'VALUE_469', 'VALUE_474',
                   'VALUE_480', 'VALUE_483', 'VALUE_484', 'VALUE_488', 'VALUE_492', 'VALUE_494', 'VALUE_496',
                   'VALUE_499', 'VALUE_502', 'VALUE_509', 'VALUE_510', 'VALUE_512', 'VALUE_513', 'VALUE_516',
                   'VALUE_517', 'VALUE_523', 'VALUE_524', 'VALUE_530', 'VALUE_531', 'VALUE_534', 'VALUE_536',
                   'VALUE_540', 'VALUE_543', 'VALUE_547', 'VALUE_550', 'VALUE_551', 'VALUE_553', 'VALUE_558',
                   'VALUE_560', 'VALUE_563', 'VALUE_566', 'VALUE_569', 'VALUE_570', 'VALUE_573', 'VALUE_576',
                   'VALUE_577', 'VALUE_579', 'VALUE_582', 'VALUE_589', 'VALUE_590', 'VALUE_591', 'VALUE_593',
                   'VALUE_595', 'VALUE_597', 'VALUE_600', 'VALUE_602', 'VALUE_603', 'VALUE_606', 'VALUE_607',
                   'VALUE_611', 'VALUE_615', 'VALUE_617', 'VALUE_618', 'VALUE_624', 'VALUE_626', 'VALUE_630',
                   'VALUE_632', 'VALUE_635', 'VALUE_636', 'VALUE_637', 'VALUE_641', 'VALUE_642', 'VALUE_644',
                   'VALUE_646', 'VALUE_647', 'VALUE_648', 'VALUE_655', 'VALUE_657', 'VALUE_658', 'VALUE_660',
                   'VALUE_662', 'VALUE_664', 'VALUE_666', 'VALUE_670', 'VALUE_674', 'VALUE_676', 'VALUE_678',
                   'VALUE_680', 'VALUE_684', 'VALUE_685', 'VALUE_690', 'VALUE_692', 'VALUE_695', 'VALUE_697',
                   'VALUE_699', 'VALUE_700', 'VALUE_702', 'VALUE_706', 'VALUE_708', 'VALUE_711', 'VALUE_713',
                   'VALUE_715', 'VALUE_720', 'VALUE_721', 'VALUE_722', 'VALUE_724', 'VALUE_725', 'VALUE_726',
                   'VALUE_729', 'VALUE_730', 'VALUE_732', 'VALUE_735', 'VALUE_737', 'VALUE_740', 'VALUE_742',
                   'VALUE_745', 'VALUE_750', 'VALUE_751', 'VALUE_752', 'VALUE_755', 'VALUE_757', 'VALUE_758',
                   'VALUE_759', 'VALUE_763', 'VALUE_764', 'VALUE_766', 'VALUE_768', 'VALUE_771', 'VALUE_774',
                   'VALUE_778', 'VALUE_780', 'VALUE_782', 'VALUE_785', 'VALUE_787', 'VALUE_789', 'VALUE_792',
                   'VALUE_794', 'VALUE_797', 'VALUE_798', 'VALUE_800', 'VALUE_804', 'VALUE_806', 'VALUE_807',
                   'VALUE_810', 'VALUE_812', 'VALUE_814', 'VALUE_816', 'VALUE_818', 'VALUE_819', 'VALUE_823',
                   'VALUE_825', 'VALUE_827', 'VALUE_829', 'VALUE_831', 'VALUE_833', 'VALUE_834', 'VALUE_835',
                   'VALUE_836', 'VALUE_840', 'VALUE_842', 'VALUE_844', 'VALUE_845', 'VALUE_846', 'VALUE_848',
                   'VALUE_849', 'VALUE_852', 'VALUE_853', 'VALUE_858', 'VALUE_859', 'VALUE_863', 'VALUE_865',
                   'VALUE_870', 'VALUE_872', 'VALUE_873', 'VALUE_874', 'VALUE_876', 'VALUE_878', 'VALUE_882',
                   'VALUE_885', 'VALUE_886', 'VALUE_888', 'VALUE_890', 'VALUE_891', 'VALUE_894', 'VALUE_898',
                   'VALUE_900', 'VALUE_901', 'VALUE_902', 'VALUE_904', 'VALUE_906', 'VALUE_907', 'VALUE_910',
                   'VALUE_912', 'VALUE_913', 'VALUE_914', 'VALUE_915', 'VALUE_917', 'VALUE_918', 'VALUE_920',
                   'VALUE_924', 'VALUE_926', 'VALUE_930', 'VALUE_931', 'VALUE_933', 'VALUE_934', 'VALUE_937',
                   'VALUE_939', 'VALUE_941', 'VALUE_942', 'VALUE_947', 'VALUE_948', 'VALUE_952', 'VALUE_953',
                   'VALUE_954', 'VALUE_956', 'VALUE_957', 'VALUE_958', 'VALUE_960', 'VALUE_961', 'VALUE_964',
                   'VALUE_966', 'VALUE_967', 'VALUE_968', 'VALUE_969', 'VALUE_971', 'VALUE_973', 'VALUE_975',
                   'VALUE_976', 'VALUE_977', 'VALUE_979', 'VALUE_980', 'VALUE_982', 'VALUE_984', 'VALUE_986',
                   'VALUE_989', 'VALUE_990', 'VALUE_991', 'VALUE_993', 'VALUE_994', 'VALUE_997', 'VALUE_998',
                   'VALUE_999', 'VALUE_1001', 'VALUE_1002', 'VALUE_1005', 'VALUE_1006', 'VALUE_1008', 'VALUE_1012',
                   'VALUE_1015', 'VALUE_1018', 'VALUE_1019', 'VALUE_1020', 'VALUE_1021', 'VALUE_1023', 'VALUE_1025',
                   'VALUE_1026', 'VALUE_1027', 'VALUE_1030', 'VALUE_1032', 'VALUE_1033', 'VALUE_1034', 'VALUE_1035',
                   'VALUE_1036', 'VALUE_1039', 'VALUE_1040', 'VALUE_1041', 'VALUE_1043', 'VALUE_1044', 'VALUE_1046',
                   'VALUE_1047', 'VALUE_1049', 'VALUE_1050', 'VALUE_1051', 'VALUE_1053', 'VALUE_1055', 'VALUE_1056',
                   'VALUE_1060', 'VALUE_1061', 'VALUE_1063', 'VALUE_1064', 'VALUE_1065', 'VALUE_1068', 'VALUE_1070',
                   'VALUE_1072', 'VALUE_1073', 'VALUE_1074', 'VALUE_1075', 'VALUE_1077', 'VALUE_1080', 'VALUE_1081',
                   'VALUE_1082', 'VALUE_1083', 'VALUE_1084', 'VALUE_1086', 'VALUE_1087', 'VALUE_1090', 'VALUE_1092',
                   'VALUE_1094', 'VALUE_1098', 'VALUE_1100', 'VALUE_1101', 'VALUE_1103', 'VALUE_1106', 'VALUE_1110',
                   'VALUE_1111', 'VALUE_1113', 'VALUE_1114', 'VALUE_1116', 'VALUE_1120', 'VALUE_1123', 'VALUE_1124',
                   'VALUE_1126', 'VALUE_1127', 'VALUE_1129', 'VALUE_1130', 'VALUE_1132', 'VALUE_1134', 'VALUE_1135',
                   'VALUE_1138', 'VALUE_1140', 'VALUE_1141', 'VALUE_1142', 'VALUE_1143', 'VALUE_1145', 'VALUE_1146',
                   'VALUE_1148', 'VALUE_1149', 'VALUE_1152', 'VALUE_1154', 'VALUE_1157', 'VALUE_1158', 'VALUE_1159',
                   'VALUE_1164', 'VALUE_1166', 'VALUE_1167', 'VALUE_1168', 'VALUE_1170', 'VALUE_1171', 'VALUE_1173',
                   'VALUE_1176', 'VALUE_1178', 'VALUE_1179', 'VALUE_1180', 'VALUE_1181', 'VALUE_1182', 'VALUE_1183',
                   'VALUE_1186', 'VALUE_1187', 'VALUE_1188', 'VALUE_1189', 'VALUE_1190', 'VALUE_1194', 'VALUE_1195',
                   'VALUE_1197', 'VALUE_1198', 'VALUE_1200', 'VALUE_1201', 'VALUE_1203', 'VALUE_1204', 'VALUE_1205',
                   'VALUE_1206', 'VALUE_1207', 'VALUE_1209', 'VALUE_1210', 'VALUE_1213', 'VALUE_1214', 'VALUE_1215',
                   'VALUE_1218', 'VALUE_1221', 'VALUE_1223', 'VALUE_1224', 'VALUE_1225', 'VALUE_1230', 'VALUE_1231',
                   'VALUE_1233', 'VALUE_1234', 'VALUE_1235', 'VALUE_1236', 'VALUE_1239', 'VALUE_1240', 'VALUE_1242',
                   'VALUE_1243', 'VALUE_1244', 'VALUE_1247', 'VALUE_1248', 'VALUE_1250', 'VALUE_1251', 'VALUE_1252',
                   'VALUE_1253', 'VALUE_1256', 'VALUE_1259', 'VALUE_1260', 'VALUE_1261', 'VALUE_1263', 'VALUE_1264',
                   'VALUE_1265', 'VALUE_1266', 'VALUE_1268', 'VALUE_1271', 'VALUE_1272', 'VALUE_1273', 'VALUE_1274',
                   'VALUE_1275', 'VALUE_1276', 'VALUE_1277', 'VALUE_1279', 'VALUE_1281', 'VALUE_1282', 'VALUE_1284',
                   'VALUE_1288', 'VALUE_1290', 'VALUE_1291', 'VALUE_1292', 'VALUE_1293', 'VALUE_1294', 'VALUE_1295',
                   'VALUE_1297', 'VALUE_1298', 'VALUE_1299', 'VALUE_1301', 'VALUE_1302', 'VALUE_1303', 'VALUE_1306',
                   'VALUE_1308', 'VALUE_1309', 'VALUE_1310', 'VALUE_1312', 'VALUE_1314', 'VALUE_1315', 'VALUE_1317',
                   'VALUE_1318', 'VALUE_1320', 'VALUE_1321', 'VALUE_1323', 'VALUE_1324', 'VALUE_1325', 'VALUE_1326',
                   'VALUE_1328', 'VALUE_1331', 'VALUE_1332', 'VALUE_1336', 'VALUE_1337', 'VALUE_1339', 'VALUE_1340',
                   'VALUE_1341', 'VALUE_1343', 'VALUE_1344', 'VALUE_1347', 'VALUE_1348', 'VALUE_1350', 'VALUE_1351',
                   'VALUE_1352', 'VALUE_1353', 'VALUE_1355', 'VALUE_1356', 'VALUE_1357', 'VALUE_1358', 'VALUE_1359',
                   'VALUE_1360', 'VALUE_1361', 'VALUE_1363', 'VALUE_1364', 'VALUE_1366', 'VALUE_1368', 'VALUE_1369',
                   'VALUE_1370', 'VALUE_1371', 'VALUE_1373', 'VALUE_1374', 'VALUE_1376', 'VALUE_1379', 'VALUE_1380',
                   'VALUE_1381', 'VALUE_1382', 'VALUE_1384', 'VALUE_1385', 'VALUE_1386', 'VALUE_1387', 'VALUE_1388',
                   'VALUE_1389', 'VALUE_1391', 'VALUE_1392', 'VALUE_1394', 'VALUE_1395', 'VALUE_1397', 'VALUE_1398',
                   'VALUE_1399', 'VALUE_1400', 'VALUE_1402', 'VALUE_1404', 'VALUE_1405', 'VALUE_1406', 'VALUE_1408',
                   'VALUE_1410', 'VALUE_1411', 'VALUE_1412', 'VALUE_1413', 'VALUE_1415', 'VALUE_1416', 'VALUE_1417',
                   'VALUE_1418', 'VALUE_1421', 'VALUE_1422', 'VALUE_1423', 'VALUE_1425', 'VALUE_1426', 'VALUE_1429',
                   'VALUE_1430', 'VALUE_1431', 'VALUE_1432', 'VALUE_1434', 'VALUE_1435', 'VALUE_1436', 'VALUE_1437',
                   'VALUE_1440', 'VALUE_1441', 'VALUE_1442', 'VALUE_1443', 'VALUE_1444', 'VALUE_1447', 'VALUE_1448',
                   'VALUE_1449', 'VALUE_1451', 'VALUE_1452', 'VALUE_1453', 'VALUE_1455', 'VALUE_1456', 'VALUE_1458',
                   'VALUE_1459', 'VALUE_1461', 'VALUE_1462', 'VALUE_1463', 'VALUE_1464', 'VALUE_1465', 'VALUE_1466',
                   'VALUE_1467', 'VALUE_1470', 'VALUE_1471', 'VALUE_1472', 'VALUE_1474', 'VALUE_1475', 'VALUE_1476',
                   'VALUE_1477', 'VALUE_1480', 'VALUE_1481', 'VALUE_1482', 'VALUE_1484', 'VALUE_1485', 'VALUE_1487',
                   'VALUE_1489', 'VALUE_1490', 'VALUE_1491', 'VALUE_1492', 'VALUE_1493', 'VALUE_1494', 'VALUE_1497',
                   'VALUE_1499', 'VALUE_1500']

grid_folder_lookup = {'AK': 'AK_WGS_1984_Albers',
                      'AS': 'AS_WGS_1984_UTM_Zone_2S',
                      'CNMI': 'CNMI_WGS_1984_UTM_Zone_55N',
                      'CONUS': 'CONUS_Albers_Conical_Equal_Area',
                      'GU': 'GU_WGS_1984_UTM_Zone_55N',
                      'HI': 'HI_NAD_1983_UTM_Zone_4N',
                      'PR': 'PR_Albers_Conical_Equal_Area',
                      'VI': 'VI_WGS_1984_UTM_Zone_20N'}

elevation_adjustments = r"E:\Workspace\StreamLine\ESA\UnionFiles_Summer2019\Range\input tables\Elevation_Summary_20180809.csv"
habitat_adjustment_path = r'E:\Workspace\StreamLine\ESA\UnionFiles_Summer2019\Range\input tables'
habitat_dict = {'AK': 'AK_Species_habitat_classes_20180624_test.csv',
                'AS': 'AS_Species_habitat_classes_20180624_test.csv',
                'CNMI': 'CNMI_Species_habitat_classes_20180624_test.csv',
                'CONUS': 'CONUS_Species_habitat_classes_20190517_test.csv',
                'GU': 'GU_Species_habitat_classes_20180624_test.csv',
                'HI': 'HI_Species_habitat_classes_20180624_test.csv',
                'PR': 'PR_Species_habitat_classes_20180624_test.csv',
                'VI': 'VI_Species_habitat_classes_20180624_test.csv'}

# SET SUFFIX FOR THE LAST FILE TO GENERATE ON LINE 635
run_habitat = True
run_elevation = False
run_elevation_hab = False
run_aqu = True

types_dict = {'VALUE': str,
              'VALUE_0': int,
              'VALUE_30': int,
              'VALUE_42': int,
              'VALUE_60': int,
              'VALUE_67': int,
              'VALUE_84': int,
              'VALUE_90': int,
              'VALUE_94': int,
              'VALUE_108': int,
              'VALUE_120': int,
              'VALUE_123': int,
              'VALUE_127': int,
              'VALUE_134': int,
              'VALUE_150': int,
              'VALUE_152': int,
              'VALUE_161': int,
              'VALUE_169': int,
              'VALUE_174': int,
              'VALUE_180': int,
              'VALUE_182': int,
              'VALUE_189': int,
              'VALUE_192': int,
              'VALUE_201': int,
              'VALUE_210': int,
              'VALUE_212': int,
              'VALUE_216': int,
              'VALUE_218': int,
              'VALUE_228': int,
              'VALUE_234': int,
              'VALUE_240': int,
              'VALUE_241': int,
              'VALUE_247': int,
              'VALUE_254': int,
              'VALUE_256': int,
              'VALUE_258': int,
              'VALUE_268': int,
              'VALUE_270': int,
              'VALUE_271': int,
              'VALUE_276': int,
              'VALUE_283': int,
              'VALUE_284': int,
              'VALUE_295': int,
              'VALUE_296': int,
              'VALUE_300': int,
              'VALUE_301': int,
              'VALUE_305': int,
              'VALUE_308': int,
              'VALUE_313': int,
              'VALUE_318': int,
              'VALUE_323': int,
              'VALUE_324': int,
              'VALUE_330': int,
              'VALUE_331': int,
              'VALUE_335': int,
              'VALUE_339': int,
              'VALUE_342': int,
              'VALUE_349': int,
              'VALUE_351': int,
              'VALUE_360': int,
              'VALUE_361': int,
              'VALUE_362': int,
              'VALUE_364': int,
              'VALUE_366': int,
              'VALUE_371': int,
              'VALUE_375': int,
              'VALUE_379': int,
              'VALUE_381': int,
              'VALUE_384': int,
              'VALUE_390': int,
              'VALUE_391': int,
              'VALUE_394': int,
              'VALUE_400': int,
              'VALUE_402': int,
              'VALUE_403': int,
              'VALUE_408': int,
              'VALUE_416': int,
              'VALUE_417': int,
              'VALUE_420': int,
              'VALUE_421': int,
              'VALUE_424': int,
              'VALUE_426': int,
              'VALUE_429': int,
              'VALUE_432': int,
              'VALUE_436': int,
              'VALUE_442': int,
              'VALUE_445': int,
              'VALUE_450': int,
              'VALUE_453': int,
              'VALUE_456': int,
              'VALUE_457': int,
              'VALUE_458': int,
              'VALUE_465': int,
              'VALUE_466': int,
              'VALUE_468': int,
              'VALUE_469': int,
              'VALUE_474': int,
              'VALUE_480': int,
              'VALUE_483': int,
              'VALUE_484': int,
              'VALUE_488': int,
              'VALUE_492': int,
              'VALUE_494': int,
              'VALUE_496': int,
              'VALUE_499': int,
              'VALUE_502': int,
              'VALUE_509': int,
              'VALUE_510': int,
              'VALUE_512': int,
              'VALUE_513': int,
              'VALUE_516': int,
              'VALUE_517': int,
              'VALUE_523': int,
              'VALUE_524': int,
              'VALUE_530': int,
              'VALUE_531': int,
              'VALUE_534': int,
              'VALUE_536': int,
              'VALUE_540': int,
              'VALUE_543': int,
              'VALUE_547': int,
              'VALUE_550': int,
              'VALUE_551': int,
              'VALUE_553': int,
              'VALUE_558': int,
              'VALUE_560': int,
              'VALUE_563': int,
              'VALUE_566': int,
              'VALUE_569': int,
              'VALUE_570': int,
              'VALUE_573': int,
              'VALUE_576': int,
              'VALUE_577': int,
              'VALUE_579': int,
              'VALUE_582': int,
              'VALUE_589': int,
              'VALUE_590': int,
              'VALUE_591': int,
              'VALUE_593': int,
              'VALUE_595': int,
              'VALUE_597': int,
              'VALUE_600': int,
              'VALUE_602': int,
              'VALUE_603': int,
              'VALUE_606': int,
              'VALUE_607': int,
              'VALUE_611': int,
              'VALUE_615': int,
              'VALUE_617': int,
              'VALUE_618': int,
              'VALUE_624': int,
              'VALUE_626': int,
              'VALUE_630': int,
              'VALUE_632': int,
              'VALUE_635': int,
              'VALUE_636': int,
              'VALUE_637': int,
              'VALUE_641': int,
              'VALUE_642': int,
              'VALUE_644': int,
              'VALUE_646': int,
              'VALUE_647': int,
              'VALUE_648': int,
              'VALUE_655': int,
              'VALUE_657': int,
              'VALUE_658': int,
              'VALUE_660': int,
              'VALUE_662': int,
              'VALUE_664': int,
              'VALUE_666': int,
              'VALUE_670': int,
              'VALUE_674': int,
              'VALUE_676': int,
              'VALUE_678': int,
              'VALUE_680': int,
              'VALUE_684': int,
              'VALUE_685': int,
              'VALUE_690': int,
              'VALUE_692': int,
              'VALUE_695': int,
              'VALUE_697': int,
              'VALUE_699': int,
              'VALUE_700': int,
              'VALUE_702': int,
              'VALUE_706': int,
              'VALUE_708': int,
              'VALUE_711': int,
              'VALUE_713': int,
              'VALUE_715': int,
              'VALUE_720': int,
              'VALUE_721': int,
              'VALUE_722': int,
              'VALUE_724': int,
              'VALUE_725': int,
              'VALUE_726': int,
              'VALUE_729': int,
              'VALUE_730': int,
              'VALUE_732': int,
              'VALUE_735': int,
              'VALUE_737': int,
              'VALUE_740': int,
              'VALUE_742': int,
              'VALUE_745': int,
              'VALUE_750': int,
              'VALUE_751': int,
              'VALUE_752': int,
              'VALUE_755': int,
              'VALUE_757': int,
              'VALUE_758': int,
              'VALUE_759': int,
              'VALUE_763': int,
              'VALUE_764': int,
              'VALUE_766': int,
              'VALUE_768': int,
              'VALUE_771': int,
              'VALUE_774': int,
              'VALUE_778': int,
              'VALUE_780': int,
              'VALUE_782': int,
              'VALUE_785': int,
              'VALUE_787': int,
              'VALUE_789': int,
              'VALUE_792': int,
              'VALUE_794': int,
              'VALUE_797': int,
              'VALUE_798': int,
              'VALUE_800': int,
              'VALUE_804': int,
              'VALUE_806': int,
              'VALUE_807': int,
              'VALUE_810': int,
              'VALUE_812': int,
              'VALUE_814': int,
              'VALUE_816': int,
              'VALUE_818': int,
              'VALUE_819': int,
              'VALUE_823': int,
              'VALUE_825': int,
              'VALUE_827': int,
              'VALUE_829': int,
              'VALUE_831': int,
              'VALUE_833': int,
              'VALUE_834': int,
              'VALUE_835': int,
              'VALUE_836': int,
              'VALUE_840': int,
              'VALUE_842': int,
              'VALUE_844': int,
              'VALUE_845': int,
              'VALUE_846': int,
              'VALUE_848': int,
              'VALUE_849': int,
              'VALUE_852': int,
              'VALUE_853': int,
              'VALUE_858': int,
              'VALUE_859': int,
              'VALUE_863': int,
              'VALUE_865': int,
              'VALUE_870': int,
              'VALUE_872': int,
              'VALUE_873': int,
              'VALUE_874': int,
              'VALUE_876': int,
              'VALUE_878': int,
              'VALUE_882': int,
              'VALUE_885': int,
              'VALUE_886': int,
              'VALUE_888': int,
              'VALUE_890': int,
              'VALUE_891': int,
              'VALUE_894': int,
              'VALUE_898': int,
              'VALUE_900': int,
              'VALUE_901': int,
              'VALUE_902': int,
              'VALUE_904': int,
              'VALUE_906': int,
              'VALUE_907': int,
              'VALUE_910': int,
              'VALUE_912': int,
              'VALUE_913': int,
              'VALUE_914': int,
              'VALUE_915': int,
              'VALUE_917': int,
              'VALUE_918': int,
              'VALUE_920': int,
              'VALUE_924': int,
              'VALUE_926': int,
              'VALUE_930': int,
              'VALUE_931': int,
              'VALUE_933': int,
              'VALUE_934': int,
              'VALUE_937': int,
              'VALUE_939': int,
              'VALUE_941': int,
              'VALUE_942': int,
              'VALUE_947': int,
              'VALUE_948': int,
              'VALUE_952': int,
              'VALUE_953': int,
              'VALUE_954': int,
              'VALUE_956': int,
              'VALUE_957': int,
              'VALUE_958': int,
              'VALUE_960': int,
              'VALUE_961': int,
              'VALUE_964': int,
              'VALUE_966': int,
              'VALUE_967': int,
              'VALUE_968': int,
              'VALUE_969': int,
              'VALUE_971': int,
              'VALUE_973': int,
              'VALUE_975': int,
              'VALUE_976': int,
              'VALUE_977': int,
              'VALUE_979': int,
              'VALUE_980': int,
              'VALUE_982': int,
              'VALUE_984': int,
              'VALUE_986': int,
              'VALUE_989': int,
              'VALUE_990': int,
              'VALUE_991': int,
              'VALUE_993': int,
              'VALUE_994': int,
              'VALUE_997': int,
              'VALUE_998': int,
              'VALUE_999': int,
              'VALUE_1001': int,
              'VALUE_1002': int,
              'VALUE_1005': int,
              'VALUE_1006': int,
              'VALUE_1008': int,
              'VALUE_1012': int,
              'VALUE_1015': int,
              'VALUE_1018': int,
              'VALUE_1019': int,
              'VALUE_1020': int,
              'VALUE_1021': int,
              'VALUE_1023': int,
              'VALUE_1025': int,
              'VALUE_1026': int,
              'VALUE_1027': int,
              'VALUE_1030': int,
              'VALUE_1032': int,
              'VALUE_1033': int,
              'VALUE_1034': int,
              'VALUE_1035': int,
              'VALUE_1036': int,
              'VALUE_1039': int,
              'VALUE_1040': int,
              'VALUE_1041': int,
              'VALUE_1043': int,
              'VALUE_1044': int,
              'VALUE_1046': int,
              'VALUE_1047': int,
              'VALUE_1049': int,
              'VALUE_1050': int,
              'VALUE_1051': int,
              'VALUE_1053': int,
              'VALUE_1055': int,
              'VALUE_1056': int,
              'VALUE_1060': int,
              'VALUE_1061': int,
              'VALUE_1063': int,
              'VALUE_1064': int,
              'VALUE_1065': int,
              'VALUE_1068': int,
              'VALUE_1070': int,
              'VALUE_1072': int,
              'VALUE_1073': int,
              'VALUE_1074': int,
              'VALUE_1075': int,
              'VALUE_1077': int,
              'VALUE_1080': int,
              'VALUE_1081': int,
              'VALUE_1082': int,
              'VALUE_1083': int,
              'VALUE_1084': int,
              'VALUE_1086': int,
              'VALUE_1087': int,
              'VALUE_1090': int,
              'VALUE_1092': int,
              'VALUE_1094': int,
              'VALUE_1098': int,
              'VALUE_1100': int,
              'VALUE_1101': int,
              'VALUE_1103': int,
              'VALUE_1106': int,
              'VALUE_1110': int,
              'VALUE_1111': int,
              'VALUE_1113': int,
              'VALUE_1114': int,
              'VALUE_1116': int,
              'VALUE_1120': int,
              'VALUE_1123': int,
              'VALUE_1124': int,
              'VALUE_1126': int,
              'VALUE_1127': int,
              'VALUE_1129': int,
              'VALUE_1130': int,
              'VALUE_1132': int,
              'VALUE_1134': int,
              'VALUE_1135': int,
              'VALUE_1138': int,
              'VALUE_1140': int,
              'VALUE_1141': int,
              'VALUE_1142': int,
              'VALUE_1143': int,
              'VALUE_1145': int,
              'VALUE_1146': int,
              'VALUE_1148': int,
              'VALUE_1149': int,
              'VALUE_1152': int,
              'VALUE_1154': int,
              'VALUE_1157': int,
              'VALUE_1158': int,
              'VALUE_1159': int,
              'VALUE_1164': int,
              'VALUE_1166': int,
              'VALUE_1167': int,
              'VALUE_1168': int,
              'VALUE_1170': int,
              'VALUE_1171': int,
              'VALUE_1173': int,
              'VALUE_1176': int,
              'VALUE_1178': int,
              'VALUE_1179': int,
              'VALUE_1180': int,
              'VALUE_1181': int,
              'VALUE_1182': int,
              'VALUE_1183': int,
              'VALUE_1186': int,
              'VALUE_1187': int,
              'VALUE_1188': int,
              'VALUE_1189': int,
              'VALUE_1190': int,
              'VALUE_1194': int,
              'VALUE_1195': int,
              'VALUE_1197': int,
              'VALUE_1198': int,
              'VALUE_1200': int,
              'VALUE_1201': int,
              'VALUE_1203': int,
              'VALUE_1204': int,
              'VALUE_1205': int,
              'VALUE_1206': int,
              'VALUE_1207': int,
              'VALUE_1209': int,
              'VALUE_1210': int,
              'VALUE_1213': int,
              'VALUE_1214': int,
              'VALUE_1215': int,
              'VALUE_1218': int,
              'VALUE_1221': int,
              'VALUE_1223': int,
              'VALUE_1224': int,
              'VALUE_1225': int,
              'VALUE_1230': int,
              'VALUE_1231': int,
              'VALUE_1233': int,
              'VALUE_1234': int,
              'VALUE_1235': int,
              'VALUE_1236': int,
              'VALUE_1239': int,
              'VALUE_1240': int,
              'VALUE_1242': int,
              'VALUE_1243': int,
              'VALUE_1244': int,
              'VALUE_1247': int,
              'VALUE_1248': int,
              'VALUE_1250': int,
              'VALUE_1251': int,
              'VALUE_1252': int,
              'VALUE_1253': int,
              'VALUE_1256': int,
              'VALUE_1259': int,
              'VALUE_1260': int,
              'VALUE_1261': int,
              'VALUE_1263': int,
              'VALUE_1264': int,
              'VALUE_1265': int,
              'VALUE_1266': int,
              'VALUE_1268': int,
              'VALUE_1271': int,
              'VALUE_1272': int,
              'VALUE_1273': int,
              'VALUE_1274': int,
              'VALUE_1275': int,
              'VALUE_1276': int,
              'VALUE_1277': int,
              'VALUE_1279': int,
              'VALUE_1281': int,
              'VALUE_1282': int,
              'VALUE_1284': int,
              'VALUE_1288': int,
              'VALUE_1290': int,
              'VALUE_1291': int,
              'VALUE_1292': int,
              'VALUE_1293': int,
              'VALUE_1294': int,
              'VALUE_1295': int,
              'VALUE_1297': int,
              'VALUE_1298': int,
              'VALUE_1299': int,
              'VALUE_1301': int,
              'VALUE_1302': int,
              'VALUE_1303': int,
              'VALUE_1306': int,
              'VALUE_1308': int,
              'VALUE_1309': int,
              'VALUE_1310': int,
              'VALUE_1312': int,
              'VALUE_1314': int,
              'VALUE_1315': int,
              'VALUE_1317': int,
              'VALUE_1318': int,
              'VALUE_1320': int,
              'VALUE_1321': int,
              'VALUE_1323': int,
              'VALUE_1324': int,
              'VALUE_1325': int,
              'VALUE_1326': int,
              'VALUE_1328': int,
              'VALUE_1331': int,
              'VALUE_1332': int,
              'VALUE_1336': int,
              'VALUE_1337': int,
              'VALUE_1339': int,
              'VALUE_1340': int,
              'VALUE_1341': int,
              'VALUE_1343': int,
              'VALUE_1344': int,
              'VALUE_1347': int,
              'VALUE_1348': int,
              'VALUE_1350': int,
              'VALUE_1351': int,
              'VALUE_1352': int,
              'VALUE_1353': int,
              'VALUE_1355': int,
              'VALUE_1356': int,
              'VALUE_1357': int,
              'VALUE_1358': int,
              'VALUE_1359': int,
              'VALUE_1360': int,
              'VALUE_1361': int,
              'VALUE_1363': int,
              'VALUE_1364': int,
              'VALUE_1366': int,
              'VALUE_1368': int,
              'VALUE_1369': int,
              'VALUE_1370': int,
              'VALUE_1371': int,
              'VALUE_1373': int,
              'VALUE_1374': int,
              'VALUE_1376': int,
              'VALUE_1379': int,
              'VALUE_1380': int,
              'VALUE_1381': int,
              'VALUE_1382': int,
              'VALUE_1384': int,
              'VALUE_1385': int,
              'VALUE_1386': int,
              'VALUE_1387': int,
              'VALUE_1388': int,
              'VALUE_1389': int,
              'VALUE_1391': int,
              'VALUE_1392': int,
              'VALUE_1394': int,
              'VALUE_1395': int,
              'VALUE_1397': int,
              'VALUE_1398': int,
              'VALUE_1399': int,
              'VALUE_1400': int,
              'VALUE_1402': int,
              'VALUE_1404': int,
              'VALUE_1405': int,
              'VALUE_1406': int,
              'VALUE_1408': int,
              'VALUE_1410': int,
              'VALUE_1411': int,
              'VALUE_1412': int,
              'VALUE_1413': int,
              'VALUE_1415': int,
              'VALUE_1416': int,
              'VALUE_1417': int,
              'VALUE_1418': int,
              'VALUE_1421': int,
              'VALUE_1422': int,
              'VALUE_1423': int,
              'VALUE_1425': int,
              'VALUE_1426': int,
              'VALUE_1429': int,
              'VALUE_1430': int,
              'VALUE_1431': int,
              'VALUE_1432': int,
              'VALUE_1434': int,
              'VALUE_1435': int,
              'VALUE_1436': int,
              'VALUE_1437': int,
              'VALUE_1440': int,
              'VALUE_1441': int,
              'VALUE_1442': int,
              'VALUE_1443': int,
              'VALUE_1444': int,
              'VALUE_1447': int,
              'VALUE_1448': int,
              'VALUE_1449': int,
              'VALUE_1451': int,
              'VALUE_1452': int,
              'VALUE_1453': int,
              'VALUE_1455': int,
              'VALUE_1456': int,
              'VALUE_1458': int,
              'VALUE_1459': int,
              'VALUE_1461': int,
              'VALUE_1462': int,
              'VALUE_1463': int,
              'VALUE_1464': int,
              'VALUE_1465': int,
              'VALUE_1466': int,
              'VALUE_1467': int,
              'VALUE_1470': int,
              'VALUE_1471': int,
              'VALUE_1472': int,
              'VALUE_1474': int,
              'VALUE_1475': int,
              'VALUE_1476': int,
              'VALUE_1477': int,
              'VALUE_1480': int,
              'VALUE_1481': int,
              'VALUE_1482': int,
              'VALUE_1484': int,
              'VALUE_1485': int,
              'VALUE_1487': int,
              'VALUE_1489': int,
              'VALUE_1490': int,
              'VALUE_1491': int,
              'VALUE_1492': int,
              'VALUE_1493': int,
              'VALUE_1494': int,
              'VALUE_1497': int,
              'VALUE_1499': int,
              'VALUE_1500': int,
              }

list_fc_region = os.listdir(look_up_fc)
list_dir = os.listdir(in_directory_csv)


def memory_check(df):
    for dtype in ['float', 'int', 'object']:
        selected_dtype = df.select_dtypes(include=[dtype])
        mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
        mean_usage_mb = mean_usage_b / 1024 ** 2
        print("Average memory usage for {} columns: {:03.2f}MB".format(dtype, mean_usage_mb))


def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


def optimize_int(df):
    # https://www.dataquest.io/blog/pandas-big-data/ website stepping out optimization
    print mem_usage(df)
    df_obj = df.select_dtypes(include=['object']).copy()
    print df_obj.describe()

    converted_obj = pd.DataFrame()
    for col in df_obj.columns:
        print col
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        if col == 'EntityID':
            pass
        elif num_unique_values / num_total_values < 0.5:  ## summarize df category if less 50% of values are unique
            converted_obj.loc[:, col] = df_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = df_obj[col]

    df.ix[:, converted_obj.columns.values.tolist()] = converted_obj.ix[:, converted_obj.columns.values.tolist()]
    del converted_obj, df_obj
    # del df_int, converted_int
    print  mem_usage(df)
    return df


def no_adjust_loop(out_df, final_df, cnty_all, sta_all):

    val_cols = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:
        val_cols.remove('VALUE')
    # Sum all value cols for a given entityID
    c_df = out_df.groupby(['EntityID'])[val_cols].sum().reset_index()
    final_df = pd.concat([final_df, c_df])  # concat if looping over table
    del c_df  # clear memory
    # Sum all value cols for a given entityID by county and state for cnty table
    df_cnty = out_df.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del out_df   # clear memory
    cnty_all = pd.concat([cnty_all, df_cnty])   # concat if looping over table
    col_order = [v for v in df_cnty if v != 'GEOID'] # get col order for states

    # Sum all value cols for a given entityID by county and state for cnty table
    df_state = df_cnty.groupby(['EntityID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del df_cnty # clear memory
    df_state = df_state.reindex(columns=col_order)  # reindex col order
    sta_all = pd.concat([sta_all, df_state])  # concat if looping over table
    del df_state # clear memory

    return final_df, cnty_all, sta_all


def adjust_elevation(out_df, adjust_path, final_df, cnty_all, sta_all):
    # Adjust for elevation based on elevation input files
    # get distance value col- everything with and _ number
    val_cols = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:  # drop the 'VALUE' col or zone identifier
        val_cols.remove('VALUE')
    # get dem col
    dem_col = [v for v in out_df.columns.values.tolist() if str(v).startswith('dem')]
    # set dem col as an int
    out_df.ix[:, dem_col[0]] = out_df.ix[:, dem_col[0]].map(lambda w: w).astype(int)
    # lookup table with elevations for a species to include
    adjust_df = pd.read_csv(adjust_path)
    index_cols = [v for v in adjust_df.columns.values.tolist() if v.startswith('Unnamed')]
    if len(index_cols) > 0:
        adjust_df.drop(index_cols, axis=1, inplace=True) # drops index rows
    # set entityid as str
    adjust_df.ix[:, 'EntityID'] = adjust_df.ix[:, 'EntityID'].map(lambda r: r.split('.')[0]).astype(str)
    sp_to_adjust = adjust_df['EntityID'].values.tolist()  # list of entity to adjust
    e_working = out_df.loc[~out_df['EntityID'].isin(sp_to_adjust)].copy()  # copy of all species with no adjustment
    e_adjust = out_df.loc[out_df['EntityID'].isin(sp_to_adjust)].copy()  # copt spece to adjust
    # do the overlap adjust to only include elevation between the min and max from look-up
    for v in sp_to_adjust:
        if v in e_adjust['EntityID'].values.tolist():
            print v # print species number
            min_v = adjust_df.loc[(adjust_df['EntityID'] == v, 'Min Elevation GIS')].iloc[0]  # get min elevation
            max_v = adjust_df.loc[(adjust_df['EntityID'] == v, 'Max Elevation GIS')].iloc[0]  # get max elevation
            print min_v, max_v  # print min and max
            # extract all row for v(species) with found between the elevations
            w_df = e_adjust.loc[(e_adjust['EntityID'] == v) & (e_adjust[dem_col[0]] <= int(max_v)) & (
                    e_adjust[dem_col[0]] >= int(min_v))].copy()

            if len(w_df) == 0:  # accounting for no overlap between the min/max overlap
                # all columns need to be the same length makes additional rows with value none
                remaining = [None] * (len(w_df.columns.values.tolist()) - 1)
                merge_list = [v] + remaining
                w_df.loc[-1] = merge_list  # adding a row
                # print w_df
            e_working = pd.concat([e_working, w_df])  # concat species to the working df with unadjusted species

    del w_df, adjust_df, sp_to_adjust, e_adjust # clears memory
    e_working['GEOID'] = e_working['GEOID'].map(lambda (n): str(n) if len(str(n)) == 5 else '0' + str(n)).astype(str)
    # sums each the distance value cols for the species across rows
    out_ele_loop = e_working.groupby('EntityID')[val_cols].sum().reset_index()
    final_df = pd.concat([final_df, out_ele_loop])  # concats if looping on tables
    del out_ele_loop  # clears memory

    # sums each the distance value cols for the species by county across rows
    df_cnty = e_working.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    cnty_all = pd.concat([cnty_all, df_cnty])  # concats if looping on tables
    col_order = [v for v in df_cnty if v != 'GEOID']

    # # sums each the distance value cols for the species by county across rowsion
    df_state = df_cnty.groupby(['EntityID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del df_cnty  # clears memory
    df_state = df_state.reindex(columns=col_order)   # reindex col order
    sta_all = pd.concat([sta_all, df_state])  # concats if looping on tables
    del df_state  # clears memory
    # retunrs e_working will have both the habitat and elevation column to so habitat can be adjusted
    return e_working, final_df, cnty_all, sta_all


def adjust_habitat(adjust_path, out_df, out_final, cnty_all, sta_all):
    # Adjusts for habitat based on habitat input file
    val_cols = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:
        val_cols.remove('VALUE')

    habitat_adjustment = adjust_path
    habitat_df = pd.read_csv(habitat_adjustment, dtype=object)
    index_cols = [v for v in habitat_df.columns.values.tolist() if v.startswith('Unnamed')]
    if len(index_cols) > 0:
        habitat_df.drop(index_cols, axis=1, inplace=True)
    sp_to_adjust_h = habitat_df.columns.values.tolist()
    hab_col = [v for v in out_df.columns.values.tolist() if str(v).startswith('Habit') or str(v).startswith(
        'gap') or str(v).startswith('2011')]

    out_df.loc[:, [hab_col[0]]] = out_df.loc[:, [hab_col[0]]].apply(lambda t: t).astype(str)
    h_working = out_df.loc[~out_df['EntityID'].isin(sp_to_adjust_h)].copy()
    h_adjust = out_df.loc[out_df['EntityID'].isin(sp_to_adjust_h)].copy()
    del out_df

    for v in sp_to_adjust_h:
        if v in h_adjust['EntityID'].values.tolist():
            hab_cat = habitat_df[v].values.tolist()
            w_df = h_adjust.loc[(h_adjust['EntityID'] == v) & (h_adjust[hab_col[0]].isin(hab_cat))].copy()
            h_working = pd.concat([h_working, w_df])
    del habitat_df


    out_hab = h_working.groupby('EntityID')[val_cols].sum().reset_index()
    out_final = pd.concat([out_final, out_hab])
    del out_hab, h_adjust,

    df_cnty = h_working.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()

    cnty_all = pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state = df_cnty.groupby(['EntityID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del df_cnty
    df_state = df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])
    del df_state

    return sp_to_adjust_h, h_working, out_final, cnty_all, sta_all


def adjust_elv_habitat(e_h_working, out_df, hab_sp_adjust, hab_df, out_final, cnty_all, sta_all):
    # Adjusts for elevation and habitat based on input file
    val_cols = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:
        val_cols.remove('VALUE')

    e_h_working = e_h_working.loc[~e_h_working['EntityID'].isin(hab_sp_adjust)].copy()
    e_h_adjust = e_h_working.loc[e_h_working['EntityID'].isin(hab_sp_adjust)].copy()

    hab_col = [v for v in out_df.columns.values.tolist() if str(v).startswith('Habit') or str(v).startswith(
        'gap') or str(v).startswith('2011')]
    out_df[hab_col[0]] = out_df[hab_col[0]].map(lambda t: t).astype(str)
    for v in hab_sp_adjust:
        if v in e_h_adjust['EntityID'].values.tolist():
            hab_cat = hab_df[v].values.tolist()
            w_eh_df = e_h_adjust.loc[(e_h_adjust['EntityID'] == v) & (e_h_adjust[hab_col[0]].isin(hab_cat))].copy()
            e_h_working = pd.concat([e_h_working, w_eh_df])

    # out_col = ['EntityID']
    # out_hab = e_h_working[out_col + val_cols]
    # group_col = [v for v in out_hab.columns.values.tolist() if not str(v).startswith("VALUE")]

    # Used the elevation adjust table, adjusted the habitat in that table, now sums to species
    out_hab = e_h_working.groupby("EntityID")[val_cols].sum().reset_index()
    out_final = pd.concat([out_final, out_hab])

    # df = e_h_working[['EntityID', 'GEOID', 'STATEFP', 'STUSPS'] + val_col].copy()
    # df_cnty = df.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    df_cnty = e_h_working.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    cnty_all = pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state = df_cnty.groupby(['EntityID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del df_cnty
    df_state = df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])
    del df_state
    out_col = ['EntityID', 'GEOID', 'STUSPS']
    [out_col.append(i) for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]

    return out_final, cnty_all, sta_all


def export_aquatics(df, df_final):
    # Export information needed for aquatic tables

    # val_col = ['EntityID', 'HUC2_AB', 'STUSPS', 'STATEFP']

    val_cols = ['EntityID', 'HUC2_AB', 'STUSPS']
    [val_cols.append(i) for i in df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:  # TODO remove in all function once it is cofirmed this col was previous dropped
        val_cols.remove('VALUE')
    aqu_group = datetime.datetime.now()
    print('Started groupby for aquatics')

    out_aqu = df.groupby(['EntityID', 'HUC2_AB', 'STUSPS'])[val_cols].sum().reset_index()

    print ("Finished group by; total time: {0}").format(datetime.datetime.now() - aqu_group)
    df_final = pd.concat([df_final, out_aqu])
    return df_final


def load_csv(in_path, spe_look_up):
    chunksize = 100000
    pp = 0
    j = 1
    print ('   Start loading csv')
    spe_look_up.ix[:, 'HUCID'] = spe_look_up.ix[:, 'HUCID'].map(lambda z: str(z).split('.')[0]).astype(str)
    all_zones_sp = spe_look_up['VALUE'].values.tolist()
    all_zones_series = pd.Series(all_zones_sp)

    for df in pd.read_csv(in_path,  engine='c', chunksize=chunksize, iterator=True, low_memory=True):
        index_cols = [v for v in df.columns.values.tolist() if v.startswith('Unnamed')]
        if len(index_cols) > 0:
            df.drop(index_cols, axis=1, inplace=True)

        if len(df) > 0:
            df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
            # df.index += j
            pp += 1
            df, dem, hab = add_spe_att(spe_look_up, df)
            if pp == 1:
                out_all = df
            else:
                out_all = pd.concat([out_all, df])
                print list(set(out_all['EntityID'].values.tolist())).sort()
                if not region == 'AK':
                    out_all = out_all.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', hab],sort=False,as_index=False).sum()
                else:
                    out_all = out_all.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB']).sum().reset_index()

            print ("    Memory usage {0}".format(mem_usage(out_all)))
            # j = df.index[-1] + 1
            print('   Finish part {0} of table'.format(pp))
            del df
        else:
            print('***No overlap for use layer - table is empty****')  # adds zeros for all species and zones if result is  no overlap
            print('***Adding in zeros****')
            df = pd.DataFrame(data=all_zones_series, columns =['VALUE'])
            df = df.reindex(columns=types_dict.keys())
            df.fillna(0,inplace=True)
            out_all, dem, hab = add_spe_att(spe_look_up, df)

    print ("    Memory usage {0}".format(mem_usage(out_all)))
    print '   Finished loading csv'
    return out_all, hab, dem



def add_spe_att( spe_look_up, raw_df):
    raw_df['VALUE'] =  raw_df['VALUE'].map(lambda k: str(k).split('.')[0]).astype(str)
    spe_look_up['VALUE'] = spe_look_up['VALUE'].map(lambda k: str(k).split('.')[0]).astype(str)
    spe_look_up.ix[:, 'HUCID'] = spe_look_up.ix[:, 'HUCID'].map(lambda z: str(z).split('.')[0]).astype('category')
    spe_look_up.ix[:, 'ZoneID'] = spe_look_up.ix[:, 'ZoneID'].map(lambda z: str(z).split('.')[0]).astype('category')
    spe_look_up.ix[:, 'EntityID'] = spe_look_up.ix[:, 'EntityID'].map(lambda z: str(z).split('.')[0]).astype(str)

    dem_col = [v for v in spe_look_up.columns.values.tolist() if str(v).startswith('dem')][0]
    if not region == 'AK':
        hab_col = [v for v in spe_look_up.columns.values.tolist() if
                   str(v).startswith('Habit') or str(v).startswith('gap') or str(v).startswith('2011')][0]
    else:
        hab_col = ''

    common_col = [col for col in raw_df.columns.values.tolist() if col in spe_look_up.columns.values.tolist()]
    # print common_col

    merge_combine = pd.merge(raw_df, spe_look_up, on=common_col, how='left')
    merge_combine.drop('VALUE', axis=1, inplace=True)
    print ("    Merged species attributes")
    del spe_look_up

    merge_combine['GEOID'] = merge_combine['GEOID'].map(lambda (n): n).astype(str)
    merge_combine.ix[:, 'GEOID'] =merge_combine.ix[:, 'GEOID'].map(lambda (n): str(n) if len(n) == 5 else '0' + str(n)).astype(str)
    merge_combine.ix[:, 'STATEFP'] =merge_combine.ix[:, 'GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
    # print merge_combine.head()

    print ("    Merged Updated HUC And State/Geo attributes")
    print ("    Completed add Attribute Info: Memory usage {0}".format(mem_usage(merge_combine)))

    val_cols = [x for x in merge_combine.columns.tolist() if str(x).startswith('VALUE')]
    # TODO ADD Elevation back in for all groupings
    # final_cols = ['EntityID', 'GEOID','STUSPS', 'STATEFP', 'HUC2_AB', dem_col,hab_col] + val_cols
    final_cols = ['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', hab_col] + val_cols
    # print final_cols
    drop_final = [v for v in merge_combine.columns.values.tolist() if v not in final_cols]
    # print drop_final
    merge_combine.drop(drop_final, axis=1, inplace=True)

    # TODO ADD Elevation back in for all groupings

    if not region == 'AK':
        # out_sp_and_huc =  merge_par.groupby(['EntityID', 'GEOID','STUSPS', 'STATEFP', 'HUC2_AB', hab_col, dem_col], sort=False, as_index= False).sum()
        out_sp_and_huc = merge_combine.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', hab_col], sort=False, as_index=False).sum()
    else:
        # out_sp_and_huc =  merge_par.groupby(['EntityID', 'GEOID','STUSPS', 'STATEFP', 'HUC2_AB', dem_col], sort=False, as_index= False).sum()
        out_sp_and_huc = merge_combine.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB'], sort=False,as_index=False).sum()
        hab_col = ''

    del merge_combine  # clear memory

    print ("    Completed groupby: Memory usage {0}".format(mem_usage(out_sp_and_huc)))

    return out_sp_and_huc, dem_col, hab_col


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

if not os.path.exists(os.path.dirname(out_path)):
    os.mkdir(os.path.dirname(out_path))

if not os.path.exists(out_path):
    os.mkdir(out_path)
list_dir = [v for v in list_dir if not v.endswith('.zip')]
print list_dir
for folder in list_dir:

    start_use = datetime.datetime.now()
    if folder in folder_skip:
        pass
    else:
        print '\nStarting use folder {0}...use {1} of {2}'.format(folder, (list_dir.index(folder) + 1), len(list_dir))
        out_folder = out_path + os.sep + 'Agg_Layers'
        if not os.path.exists(out_folder):
            os.mkdir(out_folder)

        if not os.path.exists(out_poltical):
            os.mkdir(out_poltical)

        if not os.path.exists(out_poltical + os.sep + 'States'):
            os.mkdir(out_poltical + os.sep + 'Counties')
            os.mkdir(out_poltical + os.sep + 'States')

        out_pol_folder_cny = out_poltical + os.sep + 'Counties'
        out_pol_folder_st = out_poltical + os.sep + 'States'

        region = folder.split('_')[0]  # extracts regions from folder title
        if not region in skip_regions:
            region_lookup = [v for v in list_fc_region if v.startswith(region)]

            list_fc = os.listdir(look_up_fc + os.sep + region_lookup[0])

            in_directory_species_grids = in_directory_grids + os.sep + grid_folder_lookup[
                region]  # path to region combine fld
            list_csv = os.listdir(in_directory_csv + os.sep + folder)  # list of csv in folder
            list_csv = [csv for csv in list_csv if csv.endswith('.csv')]  # list of att csvs
            # loops on each csv added the HUCIDs and ZoneIDs from parent fc att table to working dfs, then transforms table
            # so it is entityID by elevation or habitat
            for csv in list_csv:
                if csv.split("_")[0] + "_" + csv.split("_")[1] in skip_species:
                    continue
                # #TODO Update this so is dynamic based on the the true/flase of the tbale generation
                elif os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')):  # LAST CSV
                    # print out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')
                    continue
                else:
                    # print out_pol_folder_st + os.sep + csv
                    if not os.path.exists(out_pol_folder_st + os.sep + csv):
                        print ("   Working on {0}...species group {1} of {2}".format(csv, (list_csv.index(csv) + 1),
                                                                                     len(list_csv)))

                        lookup_spe = [t for t in list_fc if
                                      t.startswith(csv.split("_")[0].lower() + "_" + csv.split("_")[1].lower())]

                        if os.path.exists(look_up_fc + os.sep + region_lookup[0] + os.sep + lookup_spe[0]):
                            print look_up_fc + os.sep + region_lookup[0] + os.sep + lookup_spe[0]
                            spe_att = pd.read_csv(look_up_fc + os.sep + region_lookup[0] + os.sep + lookup_spe[0],
                                                  dtype=object)
                            index_cols = [v for v in spe_att.columns.values.tolist() if v.startswith('Unnamed')]
                            if len(index_cols) > 0:
                                spe_att.drop(index_cols, axis=1, inplace=True)
                            spe_att['VALUE'] = spe_att['VALUE'].map(lambda n: str(n).split('.')[0]).astype(str)
                            in_csv_path = in_directory_csv + os.sep + folder + os.sep + csv

                            loop_sp_table,  hab_col, dem_col = load_csv(in_csv_path, spe_att)

                            # SET COL ORDER; COL WITH NO OVERLAP NOT FOUND IN ORIGINAL OUTPUT TABLE WILL BE ADD AND NEED
                            # TO BE SET TO 0
                            if region != 'AK':
                                final_col_order_hab = final_col_order + [hab_col]  # TODO WILL NEED TO ADD DEM COL TOO
                            else:
                                final_col_order_hab = final_col_order

                            loop_sp_table = loop_sp_table.reindex(columns=final_col_order_hab)
                            loop_sp_table.fillna(0, inplace=True)  # FILLS IN EMPTY COLS WITH 0

                            time = datetime.datetime.now()
                            print(
                                "Completed loading results tables; generated working huc and species table took {0}").format(
                                (time - start_use))

                            out_habitat = pd.DataFrame(columns=[])
                            out_habitat_cnty = pd.DataFrame(columns=[])
                            out_habitat_state = pd.DataFrame(columns=[])

                            out_ele = pd.DataFrame(columns=[])
                            out_ele_cnty = pd.DataFrame(columns=[])
                            out_ele_state = pd.DataFrame(columns=[])

                            out_hab_ele = pd.DataFrame(columns=[])
                            out_hab_ele_cnty = pd.DataFrame(columns=[])
                            out_hab_ele_state = pd.DataFrame(columns=[])

                            aqu = pd.DataFrame(columns=[])

                            no_adjust_no = pd.DataFrame(columns=[])
                            no_adjust_cnty = pd.DataFrame(columns=[])
                            no_adjust_state = pd.DataFrame(columns=[])
                            # if len(loop_sp_table_huc) > 0 and len(loop_sp_table) > 0:
                            # print len(loop_sp_table)
                            if len(loop_sp_table) > 0:

                                # loop_sp_table = out_all_huc.copy()
                                # del out_all_huc
                                if not os.path.exists(out_path + os.sep + 'Agg_Layers' + os.sep + folder):
                                    os.mkdir(out_path + os.sep + 'Agg_Layers' + os.sep + folder)
                                if run_aqu:
                                    if not os.path.exists(
                                            out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                                '.csv', '_HUC2AB.csv')):
                                        # aqu = export_aquatics(loop_sp_table_huc, aqu)
                                        aqu = export_aquatics(loop_sp_table, aqu)
                                        aqu.to_csv(
                                            out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                                '.csv', '_HUC2AB.csv'))
                                        print '  Exported {0}'.format(
                                            out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                                '.csv', '_HUC2AB.csv'))

                                        print("Completed huc output and species table took {0}").format(
                                            (datetime.datetime.now() - time))
                                        time = datetime.datetime.now()
                                        del aqu
                                    else:
                                        del aqu

                                        print("Previously generated huc output and species table took {0}").format(
                                            (datetime.datetime.now() - time))
                                        time = datetime.datetime.now()

                                # loop_sp_table = out_all_other.copy()

                                if not os.path.exists(
                                        out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')):
                                    no_adjust_no, no_adjust_cnty, no_adjust_state = no_adjust_loop(loop_sp_table,
                                                                                                   no_adjust_no,
                                                                                                   no_adjust_cnty,
                                                                                                   no_adjust_state)
                                    no_adjust_no.to_csv(
                                        out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                            '.csv',
                                            '_noadjust.csv'))
                                    no_adjust_cnty.to_csv(
                                        out_pol_folder_cny + os.sep + csv.replace('.csv', '_noadjust.csv'))
                                    no_adjust_state.to_csv(
                                        out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))
                                    print '  Exported {0}'.format(
                                        out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))

                                    print("Completed no adjustment output and species table took {0}").format(
                                        (datetime.datetime.now() - time))
                                    time = datetime.datetime.now()
                                else:

                                    print(
                                        "Previously generated no adjustment output and species table took {0}").format(
                                        (datetime.datetime.now() - time))
                                    time = datetime.datetime.now()

                                del no_adjust_no, no_adjust_state, no_adjust_cnty,

                                if run_elevation:
                                    if not os.path.exists(
                                            out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEle.csv')):
                                        elev_hab_working, out_ele, out_ele_cnty, out_ele_state = adjust_elevation(
                                            loop_sp_table,
                                            elevation_adjustments, out_ele, out_ele_cnty, out_ele_state)
                                        out_ele.to_csv(
                                            out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                                '.csv', '_adjEle.csv'))
                                        out_ele_cnty.to_csv(
                                            out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjEle.csv'))
                                        out_ele_state.to_csv(
                                            out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEle.csv'))
                                        print '  Exported {0}'.format(
                                            out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEle.csv'))

                                        print("Completed elevation output and species table took {0}").format(
                                            (datetime.datetime.now() - time))
                                        time = datetime.datetime.now()

                                    else:
                                        elev_hab_working, out_ele, out_ele_cnty, out_ele_state = adjust_elevation(
                                            loop_sp_table, elevation_adjustments, out_ele, out_ele_cnty, out_ele_state)
                                        time = datetime.datetime.now()
                                        print(
                                            "Previously generated elevation output and species table took {0}").format(
                                            (datetime.datetime.now() - time))
                                    del out_ele, out_ele_cnty, out_ele_state
                                else:
                                    print ("Not generating elevation tables")

                                if region != 'AK':
                                    if run_habitat:
                                        if not os.path.exists(
                                                out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')):
                                            hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(
                                                habitat_adjustment_path + os.sep + habitat_dict[region], loop_sp_table,
                                                out_habitat, out_habitat_cnty, out_habitat_state)
                                            out_habitat.to_csv(
                                                out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                                    '.csv', '_adjHab.csv'))
                                            out_habitat_cnty.to_csv(
                                                out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjHab.csv'))
                                            out_habitat_state.to_csv(
                                                out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv'))
                                            print '  Exported {0}'.format(
                                                out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv'))

                                            print("Completed habitat output and species table took {0}").format(
                                                (datetime.datetime.now() - time))
                                            time = datetime.datetime.now()


                                        else:
                                            hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(
                                                habitat_adjustment_path + os.sep + habitat_dict[region], loop_sp_table,
                                                out_habitat, out_habitat_cnty, out_habitat_state)
                                            print(
                                                "Previously generated habitat output and species table took {0}").format(
                                                (datetime.datetime.now() - time))
                                            time = datetime.datetime.now()

                                        del out_habitat, out_habitat_cnty, out_habitat_state
                                    else:
                                        print ("Not generating habitat tables")

                                    if run_habitat and run_elevation and run_elevation_hab:
                                        if not os.path.exists(
                                                out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEleHaB.csv')):
                                            out_hab_ele, out_hab_ele_cnty, out_hab_ele_state = adjust_elv_habitat(
                                                elev_hab_working, loop_sp_table, hab_sp_adjust, hab_df, out_hab_ele,
                                                out_hab_ele_cnty, out_hab_ele_state)
                                            out_hab_ele.to_csv(
                                                out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace(
                                                    '.csv', '_adjEleHab.csv'))
                                            out_hab_ele_cnty.to_csv(
                                                out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjEleHab.csv'))
                                            out_hab_ele_state.to_csv(
                                                out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEleHab.csv'))
                                            print '  Exported {0}'.format(
                                                out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEleHaB.csv'))
                                            del out_hab_ele, out_hab_ele_cnty, out_hab_ele_state, elev_hab_working, hab_sp_adjust, hab_df
                                            print("Completed elevation output and species table took {0}").format(
                                                (datetime.datetime.now() - time))
                                        else:
                                            del out_hab_ele, out_hab_ele_cnty, out_hab_ele_state, elev_hab_working, hab_sp_adjust, hab_df
                                            print(
                                                "Previously generated habitat/elevation output: species table took {0}").format(
                                                (datetime.datetime.now() - time))
                                    else:
                                        print ("Not generating habitat/elevation tables")

                    else:
                        print (
                            'Already created all tables, the last table is: {0}'.format(
                                (out_pol_folder_st + os.sep + csv)))


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

# example of DELETED PIVOTS
# df_pivot_cnty = out_sp_table[['EntityID', 'GEOID', 'VALUE_0']].copy()
# df_pivot_cnty.ix[:, ["VALUE_0"]] = df_pivot_cnty.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
# df_pivot_cnty = df_pivot_cnty.groupby(['EntityID', 'GEOID'], as_index=False).sum()
# cnty_pivot_w = df_pivot_cnty.pivot(index='EntityID', columns='GEOID')['VALUE_0']
# out_all_cnty = pd.concat([out_all_cnty, cnty_pivot_w])
#
#
# df_pivot_st = out_sp_table[['EntityID', 'STATEFP', 'VALUE_0']].copy()
# df_pivot_st.ix[:, ["VALUE_0"]] = df_pivot_st.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
# df_pivot_st = df_pivot_st.groupby(['EntityID', 'STATEFP'], as_index=False).sum()
# sta_pivot = df_pivot_st.pivot(index='EntityID', columns='STATEFP')['VALUE_0']
# out_all_state = pd.concat([out_all_state, sta_pivot])
