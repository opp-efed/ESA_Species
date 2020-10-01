import datetime
import os

import pandas as pd
from pandas import Series, DataFrame

# Zone Look-up Table from the species input file for overlap

# look_up_fc = r'D:\Species\UnionFile_Spring2020\Range\LookUp_R_Clipped_Union_CntyInter_HUC2ABInter_20200427'
look_up_fc = r'D:\Species\UnionFile_Spring2020\Range\LookUp_Grid_byProjections_Combined_20200427'
# result library - execute for Range/Critical habitat, and L48 and NL48
in_directory_csv = r'D:\Results_Habitat\NL48\Range\Agg_Layers'
# out location - add a suffix to identify the run
out_path = r'D:\Tabulated_Habitat'
out_political = r'D:\Tabulated_Habitat\Sp_PolBoundaries'
zone_look_up_col = 'VALUE'  # typically HUCID for non habitat runs and VALUE for habitat runs

run_habitat = True# set to True if running habitat results type: bool
run_aqu = True  # set to false to skip aquatic tables type: bool
# path to tables to identify habitat values; variable will equal adjustment_inputs = '' for non-habitat runs
adjustment_inputs = r'D:\Inputs\Habitat'
habitat_adjustment_path = adjustment_inputs
# name of the habitat tables - tables can't be black add an index row if there are no habitat adjustments to make
habitat_dict = {'AK': 'AK_SpHabitat_June2020.csv',
                'AS': 'AS_SpHabitat_June2020.csv',
                'CNMI': 'CNMI_SpHabitat_June2020.csv',
                'CONUS': 'CONUS_SpHabitat_June2020.csv',
                'GU': 'GU_SpHabitat_June2020.csv',
                'HI': 'HI_SpHabitat_June2020.csv',
                'PR': 'PR_SpHabitat_June2020.csv',
                'VI': 'VI_SpHabitat_June2020.csv'}
# allows user to skip over regions
skip_regions = []
# allows user to skip over species groups
skip_species = []
# allows users to skip over UDL result folders stopped at 50
folder_skip = []  # name of use folder with results to skip over

# final column order
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
# column data types
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
# get list of files
list_fc_region = os.listdir(look_up_fc)
list_dir = os.listdir(in_directory_csv)
file_type = os.path.basename(os.path.dirname(in_directory_csv))
out_path = out_path + os.sep + 'Agg_Layers' + os.sep + file_type
if not os.path.exists(os.path.dirname(os.path.dirname(out_path))):
    os.mkdir(os.path.dirname(os.path.dirname(out_path)))
if not os.path.exists(os.path.dirname(out_path)):
    os.mkdir(os.path.dirname(out_path))
if not os.path.exists(out_path):
    os.mkdir(out_path)
nl48_regions = ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI']


def memory_check(df):
    # checks memory usage when looping over tables
    for dtype in ['float', 'int', 'object']:
        selected_dtype = df.select_dtypes(include=[dtype])
        mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
        mean_usage_mb = mean_usage_b / 1024 ** 2
        print("Average memory usage for {} columns: {:03.2f}MB".format(dtype, mean_usage_mb))


def mem_usage(pandas_obj):
    # calculates memory usage when looping over tables
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


def optimize_int(df):
    # optimization of data type in DF to help with large files
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
        elif num_unique_values / num_total_values < 0.5:  # summarize df category if less 50% of values are unique
            converted_obj.loc[:, col] = df_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = df_obj[col]

    df.ix[:, converted_obj.columns.values.tolist()] = converted_obj.ix[:, converted_obj.columns.values.tolist()]
    del converted_obj, df_obj
    # del df_int, converted_int
    print(mem_usage(df))
    return df


def no_adjust_loop(out_df, final_df, cnty_all, sta_all):
    # generate outputs with no adjustments
    val_cols = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:
        val_cols.remove('VALUE')
    # Sum all value cols for a given entityID
    c_df = out_df.groupby(['EntityID'])[val_cols].sum().reset_index()
    final_df = pd.concat([final_df, c_df])  # concat if looping over table
    del c_df  # clear memory
    # Sum all value cols for a given entityID by county and state for cnty table
    df_cnty = out_df.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del out_df  # clear memory
    cnty_all = pd.concat([cnty_all, df_cnty])  # concat if looping over table
    col_order = [u for u in df_cnty if u != 'GEOID']  # get col order for states

    # Sum all value cols for a given entityID by state  table
    df_state = df_cnty.groupby(['EntityID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del df_cnty  # clear memory
    df_state = df_state.reindex(columns=col_order)  # reindex col order
    sta_all = pd.concat([sta_all, df_state])  # concat if looping over table
    del df_state  # clear memory

    return final_df, cnty_all, sta_all


def adjust_habitat(adjust_path, out_df, out_final, cnty_all, sta_all):
    # Adjusts for habitat based on habitat input file
    val_cols = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:
        val_cols.remove('VALUE')
    # get table with the habitat code to include in the adjustment
    habitat_adjustment = adjust_path
    habitat_df = pd.read_csv(habitat_adjustment, dtype=object)

    # drops old index columns
    index_cols = [s for s in habitat_df.columns.values.tolist() if s.startswith('Unnamed')]
    if len(index_cols) > 0:
        habitat_df.drop(index_cols, axis=1, inplace=True)
    # get a list of species to adjust
    sp_to_adjust_h = habitat_df.columns.values.tolist()
    # get the hab col from table
    hab_col = [p for p in out_df.columns.values.tolist() if str(p).startswith('Habit') or str(p).startswith(
        'gap') or str(p).startswith('2011')]
    # sets the values in the habitat column to str

    out_df.loc[:, [hab_col[0]]] = out_df.loc[:, [hab_col[0]]].apply(lambda t: t).astype(str)
    # extract species values that won't be adjust and save them to a working df
    h_working = out_df.loc[~out_df['EntityID'].isin(sp_to_adjust_h)].copy()
    # extract species that will be adjusted
    h_adjust = out_df.loc[out_df['EntityID'].isin(sp_to_adjust_h)].copy()
    del out_df  # clears memory
    j_df = pd.DataFrame()
    for h in sp_to_adjust_h:
        if h in h_adjust['EntityID'].values.tolist():
            hab_cat = habitat_df[h].values.tolist()
            j_df = h_adjust.loc[(h_adjust['EntityID'] == h) & (h_adjust[hab_col[0]].isin(hab_cat))].copy()
            h_working = pd.concat([h_working, j_df])
    del habitat_df, j_df  # clears memory
    # Sums all values for each distance group by species
    out_hab = h_working.groupby('EntityID')[val_cols].sum().reset_index()
    out_final = pd.concat([out_final, out_hab])  # concat if looping over table
    del out_hab, h_adjust  # clears memory
    # Sum all value cols for a given entityID by county and state for cnty table
    df_cnty = h_working.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    cnty_all = pd.concat([cnty_all, df_cnty])  # concat if looping over table
    col_order = [q for q in df_cnty if q != 'GEOID']  # col order for state table

    # Sum all value cols for a given entityID by state  table
    df_state = df_cnty.groupby(['EntityID', 'STATEFP', 'STUSPS'])[val_cols].sum().reset_index()
    del df_cnty  # clears memory
    df_state = df_state.reindex(columns=col_order)  # reindex col order
    sta_all = pd.concat([sta_all, df_state])  # concat if looping over table
    del df_state  # clears memory

    return sp_to_adjust_h, h_working, out_final, cnty_all, sta_all


def export_aquatics(df, df_final):
    # Export information needed for aquatic tables
    val_cols = ['EntityID', 'HUC2_AB', 'STUSPS']  # columns to include in table in addition to distance cols
    [val_cols.append(i) for i in df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_cols:
        val_cols.remove('VALUE')
    aqu_group = datetime.datetime.now()
    print('Started group by for aquatics')
    # group by species huc and state across all distance cols for aquatic tables
    out_aqu = df.groupby(['EntityID', 'HUC2_AB', 'STUSPS'])[val_cols].sum().reset_index()
    print ("Finished group by; total time: {0}".format(datetime.datetime.now() - aqu_group))
    df_final = pd.concat([df_final, out_aqu])
    return df_final


def load_csv(in_path, spe_look_up, region):
    # loads in result in chuck to keep memory usage down; table can be 7+GBs
    global hab
    chunksize = 10000  # this can be increased to speed up but too high a memory errors will occur
    pp = 0  # track the number of chuck groups for the tables
    print ('   Start loading csv')
    # set identifier cols to str
    # Sets look-up values to string
    spe_look_up.ix[:, zone_look_up_col] = spe_look_up.ix[:, zone_look_up_col].map(
        lambda z: str(z).split('.')[0]).astype(str)

    if run_habitat:  # col VALUE is in the habitat look-up tables
        spe_look_up['VALUE'] = spe_look_up['VALUE'].map(lambda k: str(k).split('.')[0]).astype(str)
    else:
        # for non habitat run adds the zone look up values to table for merge
        spe_look_up.ix[:, 'VALUE'] = spe_look_up.ix[:, zone_look_up_col].map(lambda z: str(z).split('.')[0]).astype(str)

    # list of zones from species input table
    # filters based on the State abb for the current regions if the STUSPS is in the lookup table; this filtered
    # helps computer memory issues when there are a lot if zones for a species group.  Without the filter it loads
    # every zone for the species group
    if region == 'CONUS' and 'STUSPS' in spe_look_up.columns.values.tolist():
        filtered_look_up = spe_look_up[~spe_look_up['STUSPS'].isin(nl48_regions)]

        all_zones_sp = filtered_look_up[zone_look_up_col].values.tolist()
    elif region != 'CONUS' and 'STUSPS' in spe_look_up.columns.values.tolist():
        filtered_look_up = spe_look_up[spe_look_up['STUSPS'] == region]
        all_zones_sp = filtered_look_up[zone_look_up_col].values.tolist()
    else:
        filtered_look_up = spe_look_up[~spe_look_up['STUSPS'] == nl48_regions]
        all_zones_sp = filtered_look_up[zone_look_up_col].values.tolist()

    all_zones_series = pd.Series(all_zones_sp)  # type: Series # converts zones list to series
    out_all = pd.DataFrame()
    # read in df
    for df in pd.read_csv(in_path, engine='c', chunksize=chunksize, iterator=True, low_memory=True):  # type: df
        # drop old index columns loaded with header starting with 'Unnamed'
        index_cols = [q for q in df.columns.values.tolist() if q.startswith('Unnamed')]
        if len(index_cols) > 0:
            df.drop(index_cols, axis=1, inplace=True)
        # if the table as overlap the length with be greater that 0
        if len(df) > 0:
            df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
            pp += 1
            # adds in species, huc, state, cnty, hab, ele, attributes to table
            df, hab = add_spe_att(spe_look_up, df, region)
            if pp == 1:  # if this is the first loop the current df is saved as the out_all df
                out_all = df
            else:
                out_all = pd.concat([out_all, df])  # type: df # current df is concat to the out_all
                if run_habitat:
                    if not region == 'AK':  # no habitat file in AK so it can't have the hab cal
                        # sum all values by species cnty stat huc combos  for each val dis cols
                        out_all = out_all.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', hab],
                                                  sort=False, as_index=False).sum()
                else:
                    out_all = out_all.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB']).sum().reset_index()
            # checks memory usage
            print ("    Memory usage {0}".format(mem_usage(out_all)))
            print('   Finish part {0} of table'.format(pp))
            del df
        else:
            print('***No overlap for use layer - table is empty****')
            print('***Adding in zeros****')
            # adds zeros for all ones if loaded result has no overlap
            df = pd.DataFrame(data=all_zones_series, columns=['VALUE'])
            df = df.reindex(columns=types_dict.keys())  # set columns
            df.fillna(0, inplace=True)  # fills blanks with 0s
            # adds in species attributes
            out_all, hab = add_spe_att(spe_look_up, df, region)
    print ("    Memory usage {0}".format(mem_usage(out_all)))
    print '   Finished loading csv'
    return out_all, hab


def add_spe_att(spe_look_up, raw_df, region):
    # sets identifiers to strs
    # this is the zone column in the result tables
    raw_df['VALUE'] = raw_df['VALUE'].map(lambda k: str(k).split('.')[0]).astype(str)
    spe_look_up.ix[:, 'HUCID'] = spe_look_up.ix[:, 'HUCID'].map(lambda z: str(z).split('.')[0]).astype('category')
    spe_look_up.ix[:, 'ZoneID'] = spe_look_up.ix[:, 'ZoneID'].map(lambda z: str(z).split('.')[0]).astype('category')
    spe_look_up.ix[:, 'EntityID'] = spe_look_up.ix[:, 'EntityID'].map(lambda z: str(z).split('.')[0]).astype(str)
    hab_col = ''
    out_sp_and_huc = pd.DataFrame()

    # extract hab col from attributes
    if run_habitat:
        if not region == 'AK':  # no hab data for AK
            hab_col = [t for t in spe_look_up.columns.values.tolist() if
                       str(t).startswith('Habit') or str(t).startswith('gap') or str(t).startswith('2011')][0]

    # id's common cols from attributes and results to join on
    common_col = [col for col in raw_df.columns.values.tolist() if col in spe_look_up.columns.values.tolist()]
    # merges results and attributes using a left join (ie all values from result included (raw_df) and corresponding
    # values from attributes loaded
    merge_combine = pd.merge(raw_df, spe_look_up, on=common_col, how='left')
    merge_combine.drop('VALUE', axis=1, inplace=True)  # drops val col; don't need once attributes are loaded
    print ("    Merged species attributes")
    del spe_look_up  # clears memories
    # updates the GEOID And STATEFP identifiers
    merge_combine.ix[:, 'GEOID'] = merge_combine.ix[:, 'GEOID'].map(
        lambda (n): str(n) if len(n) == 5 else '0' + str(n)).astype(str)
    merge_combine.ix[:, 'STATEFP'] = merge_combine.ix[:, 'GEOID'].map(
        lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    print ("    Merged Updated HUC And State/Geo attributes")
    print ("    Completed add Attribute Info: Memory usage {0}".format(mem_usage(merge_combine)))

    # extract value distance columns - ie overlap results; and other cols need in the final output
    val_cols = [x for x in merge_combine.columns.tolist() if str(x).startswith('VALUE')]

    final_cols = ['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', hab_col] + val_cols
    # ids cols not part of the final output
    drop_final = [t for t in merge_combine.columns.values.tolist() if t not in final_cols]
    # drops cols that aren't needed
    merge_combine.drop(drop_final, axis=1, inplace=True)
    if run_habitat:
        if not region == 'AK':  # AK doesn't have hab
            out_sp_and_huc = merge_combine.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB', hab_col],
                                                   sort=False, as_index=False).sum()
    else:
        out_sp_and_huc = merge_combine.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP', 'HUC2_AB'], sort=False,
                                               as_index=False).sum()
        hab_col = ''

    del merge_combine  # clear memory
    print ("    Completed group by: Memory usage {0}".format(mem_usage(out_sp_and_huc)))
    return out_sp_and_huc, hab_col


def export_tables(loop_sp_table, folder, csv, time, out_pol_folder_st, out_pol_folder_cny, region):
    # exports working tables summarized to species
    global hab_df, hab_sp_adjust, hab_df, hab_sp_adjust
    out_habitat = pd.DataFrame(columns=[])  # type: DataFrame # empty dataframe to store final table if looping
    out_habitat_cnty = pd.DataFrame(columns=[])  # type: DataFrame
    out_habitat_state = pd.DataFrame(columns=[])  # type: DataFrame

    aqu = pd.DataFrame(columns=[])  # type: DataFrame

    no_adjust_no = pd.DataFrame(columns=[])  # type: DataFrame
    no_adjust_cnty = pd.DataFrame(columns=[])  # type: DataFrame
    no_adjust_state = pd.DataFrame(columns=[])  # type: DataFrame

    if len(loop_sp_table) > 0:
        # generated output folder if needed
        if not os.path.exists(out_path + os.sep + folder):
            os.mkdir(out_path + os.sep + folder)
        if run_aqu:  # export aquatic tables if boolean set to turn to include  aquatic tables
            if not os.path.exists(out_path + os.sep + folder + os.sep + csv.replace('.csv', '_HUC2AB.csv')):
                # Generates aquatic tables
                aqu = export_aquatics(loop_sp_table, aqu)
                # saves to csv
                aqu.to_csv(out_path + os.sep + folder + os.sep + csv.replace('.csv', '_HUC2AB.csv'))
                print (
                    '  Exported {0}'.format(out_path + os.sep + folder + os.sep + csv.replace('.csv', '_HUC2AB.csv')))
                print("Completed huc output and species table took {0}".format((datetime.datetime.now() - time)))
                del aqu  # clear memory
            else:
                del aqu  # clear memory
                print("Previously generated huc output and species table took {0}".format(
                    (datetime.datetime.now() - time)))

        # export tables with no adjustments made
        if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')):
            # generates tables with not adjustment
            no_adjust_no, no_adjust_cnty, no_adjust_state = no_adjust_loop(loop_sp_table, no_adjust_no,
                                                                           no_adjust_cnty,
                                                                           no_adjust_state)
            # saves to csv
            no_adjust_no.to_csv(out_path + os.sep + folder + os.sep + csv.replace('.csv', '_noadjust.csv'))
            no_adjust_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_noadjust.csv'))
            no_adjust_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))
            print ('  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')))
            print("Completed no adjustment output and species table took {0}".format(
                (datetime.datetime.now() - time)))

        else:
            print("Previously generated no adjustment output and species table took {0}".format(
                (datetime.datetime.now() - time)))

        del no_adjust_no, no_adjust_state, no_adjust_cnty  # clears memory

        if region != 'AK':  # no hab files in AK
            if run_habitat:  # generated habitat tables if habitat boolean set to true
                if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')):
                    # generated habitat tables if habitat boolean set to true
                    hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(
                        habitat_adjustment_path + os.sep + habitat_dict[region], loop_sp_table,
                        out_habitat, out_habitat_cnty, out_habitat_state)
                    # saves to csvs
                    out_habitat.to_csv(out_path + os.sep + folder + os.sep + csv.replace('.csv', '_adjHab.csv'))
                    out_habitat_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjHab.csv'))
                    out_habitat_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv'))
                    print ('  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')))
                    print("Completed habitat output and species table took {0}".format(
                        (datetime.datetime.now() - time)))

                else:  # hab files already save but need working files for adjusting by hab and elevation
                    hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(
                        habitat_adjustment_path + os.sep + habitat_dict[region], loop_sp_table,
                        out_habitat, out_habitat_cnty, out_habitat_state)
                    print("Previously generated habitat output and species table took {0}".format(
                        (datetime.datetime.now() - time)))

                del out_habitat, out_habitat_cnty, out_habitat_state  # clears memory
            else:
                print ("Not generating habitat tables")
    else:
        print ('Table has no records')


def main(list_dir_results):
    for folder in list_dir_results:  # loops over folders with results
        start_use = datetime.datetime.now()  # get time for tracker
        if folder in folder_skip:  # skip folder passed on user parameters
            pass
        else:
            print ('\nStarting use folder {0}...use {1} of {2}'.format(folder, (list_dir.index(folder) + 1),
                                                                       len(list_dir)))
            # set output folder
            out_folder = out_path
            out_pol_folder_cny = out_political + os.sep + 'Counties'
            out_pol_folder_st = out_political + os.sep + 'States'
            # generated folders need for the output if they don't exists
            if not os.path.exists(out_folder):  # species
                os.mkdir(out_folder)
            if not os.path.exists(out_political):  # political boundary results
                os.mkdir(out_political)
            if not os.path.exists(out_political + os.sep + 'States'):  # State and county folders
                os.mkdir(out_political + os.sep + 'Counties')
                os.mkdir(out_political + os.sep + 'States')

            region = folder.split('_')[0]  # extracts regions from folder title
            if region not in skip_regions:
                if run_habitat:
                    # get a list of species group for regions
                    region_lookup = [t for t in list_fc_region if t.startswith(region)]
                    list_fc = os.listdir(look_up_fc + os.sep + region_lookup[0])
                    look_up_path = look_up_fc + os.sep + region_lookup[0]
                else:
                    # get a list of species group
                    list_fc = os.listdir(look_up_fc)
                    look_up_path = look_up_fc

                # gets a path to current folder of csvs in folder and gets list of csvs
                list_csv = os.listdir(in_directory_csv + os.sep + folder)
                list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
                # loops on each csv adds species, huc, political boundary, habitat from parent fc
                # att table to working results dfs, then transforms tables for outputs
                # so it is entityID by elevation or habitat
                for csv in list_csv:
                    # check for table that already exists so they won't be re-run; if the final csv generated by script
                    # already exists the folder will be skipped
                    if run_habitat:
                        final_csv = out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')

                    else:
                        final_csv = out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')
                    if os.path.exists(final_csv):  # LAST CSV
                        print ('Already created all tables, the last table is: {0}'.format(final_csv))
                        continue
                    elif csv.split("_")[0] + "_" + csv.split("_")[1] in skip_species:
                        # skips species if in the skip species parameter set by user
                        continue
                    else:  # if tables needed to be generated
                        if not os.path.exists(out_pol_folder_st + os.sep + csv):
                            print ("   Working on {0}...species group {1} of {2}".format(csv, (list_csv.index(csv) + 1),
                                                                                         len(list_csv)))
                            if run_habitat:
                                lookup_spe = [t for t in list_fc if
                                              t.startswith(csv.split("_")[0].lower() + "_" + csv.split("_")[1].lower())]
                            else:
                                lookup_spe = [t for t in list_fc if
                                              t.startswith(
                                                  csv.split("_")[0].upper() + "_" + csv.split("_")[1].capitalize())]

                            if os.path.exists(look_up_path + os.sep + lookup_spe[0]):
                                # print look_up_fc + os.sep + region_lookup[0] + os.sep + lookup_spe[0]
                                # read in the look_up table for species will of the att to be added to results
                                spe_att = pd.read_csv(look_up_path + os.sep + lookup_spe[0], dtype=object)
                                # drops any old index cols that start with unnamed
                                index_cols = [t for t in spe_att.columns.values.tolist() if t.startswith('Unnamed')]
                                if len(index_cols) > 0:
                                    spe_att.drop(index_cols, axis=1, inplace=True)
                                # path to results csv
                                in_csv_path = in_directory_csv + os.sep + folder + os.sep + csv
                                # loads results csv in chucks due to file sizes
                                loop_sp_table, hab_col = load_csv(in_csv_path, spe_att, region)

                                if region != 'AK':  # No hab files in AK
                                    final_col_order_att = final_col_order + [hab_col]
                                else:
                                    final_col_order_att = final_col_order

                                loop_sp_table = loop_sp_table.reindex(columns=final_col_order_att)
                                loop_sp_table.fillna(0, inplace=True)  # Fills in empty cells with 0

                                print("Completed loading results tables with huc and species took {0}".format(
                                    (datetime.datetime.now() - start_use)))
                                export_tables(loop_sp_table, folder, csv, start_use, out_pol_folder_st,
                                              out_pol_folder_cny, region)


# start clock
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# generated the output folders if needed
if not os.path.exists(os.path.dirname(out_path)):
    os.mkdir(os.path.dirname(out_path))

if not os.path.exists(out_path):
    os.mkdir(out_path)
# get list of directory with results; excludes .zip used to back up of results
list_dir = [v for v in list_dir if not v.endswith('.zip')]
print list_dir
main(list_dir)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
