import pandas as pd

import datetime
import os

# result library
in_directory_csv = r'path\L48\Range\Agg_Layers'
# look up directory
look_up_fc_ab = r'path\Lookup_directory'

out_path = r'outpath\folder'
out_poltical = r'outpath\folder\PolBoundaries'

# species grid files by regions
grid_folder_lookup = {'AK': 'AK_WGS_1984_Albers',
                      'AS': 'AS_WGS_1984_UTM_Zone_2S',
                      'CNMI': 'CNMI_WGS_1984_UTM_Zone_55N',
                      'CONUS': 'CONUS_Albers_Conical_Equal_Area',
                      'GU': 'GU_WGS_1984_UTM_Zone_55N',
                      'HI': 'HI_NAD_1983_UTM_Zone_4N',
                      'PR': 'PR_Albers_Conical_Equal_Area',
                      'VI': 'VI_WGS_1984_UTM_Zone_20N'}
# final cols
final_cols = ['EntityID', 'STUSPS', 'HUC2_AB', 'VALUE_0', 'VALUE_30', 'VALUE_42', 'VALUE_60', 'VALUE_67', 'VALUE_84',
              'VALUE_90', 'VALUE_94', 'VALUE_108', 'VALUE_120', 'VALUE_123', 'VALUE_127', 'VALUE_134', 'VALUE_150',
              'VALUE_152', 'VALUE_161', 'VALUE_169', 'VALUE_174', 'VALUE_180', 'VALUE_182', 'VALUE_189', 'VALUE_192',
              'VALUE_201', 'VALUE_210', 'VALUE_212', 'VALUE_216', 'VALUE_218', 'VALUE_228', 'VALUE_234', 'VALUE_240',
              'VALUE_241', 'VALUE_247', 'VALUE_254', 'VALUE_256', 'VALUE_258', 'VALUE_268', 'VALUE_270', 'VALUE_271',
              'VALUE_276', 'VALUE_283', 'VALUE_284', 'VALUE_295', 'VALUE_296', 'VALUE_300', 'VALUE_301', 'VALUE_305',
              'VALUE_308', 'VALUE_313', 'VALUE_318', 'VALUE_323', 'VALUE_324', 'VALUE_330', 'VALUE_331', 'VALUE_335',
              'VALUE_339', 'VALUE_342', 'VALUE_349', 'VALUE_351', 'VALUE_360', 'VALUE_361', 'VALUE_362', 'VALUE_364',
              'VALUE_366', 'VALUE_371', 'VALUE_375', 'VALUE_379', 'VALUE_381', 'VALUE_384', 'VALUE_390', 'VALUE_391',
              'VALUE_394', 'VALUE_400', 'VALUE_402', 'VALUE_403', 'VALUE_408', 'VALUE_416', 'VALUE_417', 'VALUE_420',
              'VALUE_421', 'VALUE_424', 'VALUE_426', 'VALUE_429', 'VALUE_432', 'VALUE_436', 'VALUE_442', 'VALUE_445',
              'VALUE_450', 'VALUE_453', 'VALUE_456', 'VALUE_457', 'VALUE_458', 'VALUE_465', 'VALUE_466', 'VALUE_468',
              'VALUE_469', 'VALUE_474', 'VALUE_480', 'VALUE_483', 'VALUE_484', 'VALUE_488', 'VALUE_492', 'VALUE_494',
              'VALUE_496', 'VALUE_499', 'VALUE_502', 'VALUE_509', 'VALUE_510', 'VALUE_512', 'VALUE_513', 'VALUE_516',
              'VALUE_517', 'VALUE_523', 'VALUE_524', 'VALUE_530', 'VALUE_531', 'VALUE_534', 'VALUE_536', 'VALUE_540',
              'VALUE_543', 'VALUE_547', 'VALUE_550', 'VALUE_551', 'VALUE_553', 'VALUE_558', 'VALUE_560', 'VALUE_563',
              'VALUE_566', 'VALUE_569', 'VALUE_570', 'VALUE_573', 'VALUE_576', 'VALUE_577', 'VALUE_579', 'VALUE_582',
              'VALUE_589', 'VALUE_590', 'VALUE_591', 'VALUE_593', 'VALUE_595', 'VALUE_597', 'VALUE_600', 'VALUE_602',
              'VALUE_603', 'VALUE_606', 'VALUE_607', 'VALUE_611', 'VALUE_615', 'VALUE_617', 'VALUE_618', 'VALUE_624',
              'VALUE_626', 'VALUE_630', 'VALUE_632', 'VALUE_635', 'VALUE_636', 'VALUE_637', 'VALUE_641', 'VALUE_642',
              'VALUE_644', 'VALUE_646', 'VALUE_647', 'VALUE_648', 'VALUE_655', 'VALUE_657', 'VALUE_658', 'VALUE_660',
              'VALUE_662', 'VALUE_664', 'VALUE_666', 'VALUE_670', 'VALUE_674', 'VALUE_676', 'VALUE_678', 'VALUE_680',
              'VALUE_684', 'VALUE_685', 'VALUE_690', 'VALUE_692', 'VALUE_695', 'VALUE_697', 'VALUE_699', 'VALUE_700',
              'VALUE_702', 'VALUE_706', 'VALUE_708', 'VALUE_711', 'VALUE_713', 'VALUE_715', 'VALUE_720', 'VALUE_721',
              'VALUE_722', 'VALUE_724', 'VALUE_725', 'VALUE_726', 'VALUE_729', 'VALUE_730', 'VALUE_732', 'VALUE_735',
              'VALUE_737', 'VALUE_740', 'VALUE_742', 'VALUE_745', 'VALUE_750', 'VALUE_751', 'VALUE_752', 'VALUE_755',
              'VALUE_757', 'VALUE_758', 'VALUE_759', 'VALUE_763', 'VALUE_764', 'VALUE_766', 'VALUE_768', 'VALUE_771',
              'VALUE_774', 'VALUE_778', 'VALUE_780', 'VALUE_782', 'VALUE_785', 'VALUE_787', 'VALUE_789', 'VALUE_792',
              'VALUE_794', 'VALUE_797', 'VALUE_798', 'VALUE_800', 'VALUE_804', 'VALUE_806', 'VALUE_807', 'VALUE_810',
              'VALUE_812', 'VALUE_814', 'VALUE_816', 'VALUE_818', 'VALUE_819', 'VALUE_823', 'VALUE_825', 'VALUE_827',
              'VALUE_829', 'VALUE_831', 'VALUE_833', 'VALUE_834', 'VALUE_835', 'VALUE_836', 'VALUE_840', 'VALUE_842',
              'VALUE_844', 'VALUE_845', 'VALUE_846', 'VALUE_848', 'VALUE_849', 'VALUE_852', 'VALUE_853', 'VALUE_858',
              'VALUE_859', 'VALUE_863', 'VALUE_865', 'VALUE_870', 'VALUE_872', 'VALUE_873', 'VALUE_874', 'VALUE_876',
              'VALUE_878', 'VALUE_882', 'VALUE_885', 'VALUE_886', 'VALUE_888', 'VALUE_890', 'VALUE_891', 'VALUE_894',
              'VALUE_898', 'VALUE_900', 'VALUE_901', 'VALUE_902', 'VALUE_904', 'VALUE_906', 'VALUE_907', 'VALUE_910',
              'VALUE_912', 'VALUE_913', 'VALUE_914', 'VALUE_915', 'VALUE_917', 'VALUE_918', 'VALUE_920', 'VALUE_924',
              'VALUE_926', 'VALUE_930', 'VALUE_931', 'VALUE_933', 'VALUE_934', 'VALUE_937', 'VALUE_939', 'VALUE_941',
              'VALUE_942', 'VALUE_947', 'VALUE_948', 'VALUE_952', 'VALUE_953', 'VALUE_954', 'VALUE_956', 'VALUE_957',
              'VALUE_958', 'VALUE_960', 'VALUE_961', 'VALUE_964', 'VALUE_966', 'VALUE_967', 'VALUE_968', 'VALUE_969',
              'VALUE_971', 'VALUE_973', 'VALUE_975', 'VALUE_976', 'VALUE_977', 'VALUE_979', 'VALUE_980', 'VALUE_982',
              'VALUE_984', 'VALUE_986', 'VALUE_989', 'VALUE_990', 'VALUE_991', 'VALUE_993', 'VALUE_994', 'VALUE_997',
              'VALUE_998', 'VALUE_999', 'VALUE_1001', 'VALUE_1002', 'VALUE_1005', 'VALUE_1006', 'VALUE_1008',
              'VALUE_1012', 'VALUE_1015', 'VALUE_1018', 'VALUE_1019', 'VALUE_1020', 'VALUE_1021', 'VALUE_1023',
              'VALUE_1025', 'VALUE_1026', 'VALUE_1027', 'VALUE_1030', 'VALUE_1032', 'VALUE_1033', 'VALUE_1034',
              'VALUE_1035', 'VALUE_1036', 'VALUE_1039', 'VALUE_1040', 'VALUE_1041', 'VALUE_1043', 'VALUE_1044',
              'VALUE_1046', 'VALUE_1047', 'VALUE_1049', 'VALUE_1050', 'VALUE_1051', 'VALUE_1053', 'VALUE_1055',
              'VALUE_1056', 'VALUE_1060', 'VALUE_1061', 'VALUE_1063', 'VALUE_1064', 'VALUE_1065', 'VALUE_1068',
              'VALUE_1070', 'VALUE_1072', 'VALUE_1073', 'VALUE_1074', 'VALUE_1075', 'VALUE_1077', 'VALUE_1080',
              'VALUE_1081', 'VALUE_1082', 'VALUE_1083', 'VALUE_1084', 'VALUE_1086', 'VALUE_1087', 'VALUE_1090',
              'VALUE_1092', 'VALUE_1094', 'VALUE_1098', 'VALUE_1100', 'VALUE_1101', 'VALUE_1103', 'VALUE_1106',
              'VALUE_1110', 'VALUE_1111', 'VALUE_1113', 'VALUE_1114', 'VALUE_1116', 'VALUE_1120', 'VALUE_1123',
              'VALUE_1124', 'VALUE_1126', 'VALUE_1127', 'VALUE_1129', 'VALUE_1130', 'VALUE_1132', 'VALUE_1134',
              'VALUE_1135', 'VALUE_1138', 'VALUE_1140', 'VALUE_1141', 'VALUE_1142', 'VALUE_1143', 'VALUE_1145',
              'VALUE_1146', 'VALUE_1148', 'VALUE_1149', 'VALUE_1152', 'VALUE_1154', 'VALUE_1157', 'VALUE_1158',
              'VALUE_1159', 'VALUE_1164', 'VALUE_1166', 'VALUE_1167', 'VALUE_1168', 'VALUE_1170', 'VALUE_1171',
              'VALUE_1173', 'VALUE_1176', 'VALUE_1178', 'VALUE_1179', 'VALUE_1180', 'VALUE_1181', 'VALUE_1182',
              'VALUE_1183', 'VALUE_1186', 'VALUE_1187', 'VALUE_1188', 'VALUE_1189', 'VALUE_1190', 'VALUE_1194',
              'VALUE_1195', 'VALUE_1197', 'VALUE_1198', 'VALUE_1200', 'VALUE_1201', 'VALUE_1203', 'VALUE_1204',
              'VALUE_1205', 'VALUE_1206', 'VALUE_1207', 'VALUE_1209', 'VALUE_1210', 'VALUE_1213', 'VALUE_1214',
              'VALUE_1215', 'VALUE_1218', 'VALUE_1221', 'VALUE_1223', 'VALUE_1224', 'VALUE_1225', 'VALUE_1230',
              'VALUE_1231', 'VALUE_1233', 'VALUE_1234', 'VALUE_1235', 'VALUE_1236', 'VALUE_1239', 'VALUE_1240',
              'VALUE_1242', 'VALUE_1243', 'VALUE_1244', 'VALUE_1247', 'VALUE_1248', 'VALUE_1250', 'VALUE_1251',
              'VALUE_1252', 'VALUE_1253', 'VALUE_1256', 'VALUE_1259', 'VALUE_1260', 'VALUE_1261', 'VALUE_1263',
              'VALUE_1264', 'VALUE_1265', 'VALUE_1266', 'VALUE_1268', 'VALUE_1271', 'VALUE_1272', 'VALUE_1273',
              'VALUE_1274', 'VALUE_1275', 'VALUE_1276', 'VALUE_1277', 'VALUE_1279', 'VALUE_1281', 'VALUE_1282',
              'VALUE_1284', 'VALUE_1288', 'VALUE_1290', 'VALUE_1291', 'VALUE_1292', 'VALUE_1293', 'VALUE_1294',
              'VALUE_1295', 'VALUE_1297', 'VALUE_1298', 'VALUE_1299', 'VALUE_1301', 'VALUE_1302', 'VALUE_1303',
              'VALUE_1306', 'VALUE_1308', 'VALUE_1309', 'VALUE_1310', 'VALUE_1312', 'VALUE_1314', 'VALUE_1315',
              'VALUE_1317', 'VALUE_1318', 'VALUE_1320', 'VALUE_1321', 'VALUE_1323', 'VALUE_1324', 'VALUE_1325',
              'VALUE_1326', 'VALUE_1328', 'VALUE_1331', 'VALUE_1332', 'VALUE_1336', 'VALUE_1337', 'VALUE_1339',
              'VALUE_1340', 'VALUE_1341', 'VALUE_1343', 'VALUE_1344', 'VALUE_1347', 'VALUE_1348', 'VALUE_1350',
              'VALUE_1351', 'VALUE_1352', 'VALUE_1353', 'VALUE_1355', 'VALUE_1356', 'VALUE_1357', 'VALUE_1358',
              'VALUE_1359', 'VALUE_1360', 'VALUE_1361', 'VALUE_1363', 'VALUE_1364', 'VALUE_1366', 'VALUE_1368',
              'VALUE_1369', 'VALUE_1370', 'VALUE_1371', 'VALUE_1373', 'VALUE_1374', 'VALUE_1376', 'VALUE_1379',
              'VALUE_1380', 'VALUE_1381', 'VALUE_1382', 'VALUE_1384', 'VALUE_1385', 'VALUE_1386', 'VALUE_1387',
              'VALUE_1388', 'VALUE_1389', 'VALUE_1391', 'VALUE_1392', 'VALUE_1394', 'VALUE_1395', 'VALUE_1397',
              'VALUE_1398', 'VALUE_1399', 'VALUE_1400', 'VALUE_1402', 'VALUE_1404', 'VALUE_1405', 'VALUE_1406',
              'VALUE_1408', 'VALUE_1410', 'VALUE_1411', 'VALUE_1412', 'VALUE_1413', 'VALUE_1415', 'VALUE_1416',
              'VALUE_1417', 'VALUE_1418', 'VALUE_1421', 'VALUE_1422', 'VALUE_1423', 'VALUE_1425', 'VALUE_1426',
              'VALUE_1429', 'VALUE_1430', 'VALUE_1431', 'VALUE_1432', 'VALUE_1434', 'VALUE_1435', 'VALUE_1436',
              'VALUE_1437', 'VALUE_1440', 'VALUE_1441', 'VALUE_1442', 'VALUE_1443', 'VALUE_1444', 'VALUE_1447',
              'VALUE_1448', 'VALUE_1449', 'VALUE_1451', 'VALUE_1452', 'VALUE_1453', 'VALUE_1455', 'VALUE_1456',
              'VALUE_1458', 'VALUE_1459', 'VALUE_1461', 'VALUE_1462', 'VALUE_1463', 'VALUE_1464', 'VALUE_1465',
              'VALUE_1466', 'VALUE_1467', 'VALUE_1470', 'VALUE_1471', 'VALUE_1472', 'VALUE_1474', 'VALUE_1475',
              'VALUE_1476', 'VALUE_1477', 'VALUE_1480', 'VALUE_1481', 'VALUE_1482', 'VALUE_1484', 'VALUE_1485',
              'VALUE_1487', 'VALUE_1489', 'VALUE_1490', 'VALUE_1491', 'VALUE_1492', 'VALUE_1493', 'VALUE_1494',
              'VALUE_1497', 'VALUE_1499', 'VALUE_1500']

final_cols_all = ['EntityID', 'GEOID', 'STUSPS', 'STATEFP','VALUE_0', 'VALUE_30', 'VALUE_42', 'VALUE_60', 'VALUE_67', 'VALUE_84',
              'VALUE_90', 'VALUE_94', 'VALUE_108', 'VALUE_120', 'VALUE_123', 'VALUE_127', 'VALUE_134', 'VALUE_150',
              'VALUE_152', 'VALUE_161', 'VALUE_169', 'VALUE_174', 'VALUE_180', 'VALUE_182', 'VALUE_189', 'VALUE_192',
              'VALUE_201', 'VALUE_210', 'VALUE_212', 'VALUE_216', 'VALUE_218', 'VALUE_228', 'VALUE_234', 'VALUE_240',
              'VALUE_241', 'VALUE_247', 'VALUE_254', 'VALUE_256', 'VALUE_258', 'VALUE_268', 'VALUE_270', 'VALUE_271',
              'VALUE_276', 'VALUE_283', 'VALUE_284', 'VALUE_295', 'VALUE_296', 'VALUE_300', 'VALUE_301', 'VALUE_305',
              'VALUE_308', 'VALUE_313', 'VALUE_318', 'VALUE_323', 'VALUE_324', 'VALUE_330', 'VALUE_331', 'VALUE_335',
              'VALUE_339', 'VALUE_342', 'VALUE_349', 'VALUE_351', 'VALUE_360', 'VALUE_361', 'VALUE_362', 'VALUE_364',
              'VALUE_366', 'VALUE_371', 'VALUE_375', 'VALUE_379', 'VALUE_381', 'VALUE_384', 'VALUE_390', 'VALUE_391',
              'VALUE_394', 'VALUE_400', 'VALUE_402', 'VALUE_403', 'VALUE_408', 'VALUE_416', 'VALUE_417', 'VALUE_420',
              'VALUE_421', 'VALUE_424', 'VALUE_426', 'VALUE_429', 'VALUE_432', 'VALUE_436', 'VALUE_442', 'VALUE_445',
              'VALUE_450', 'VALUE_453', 'VALUE_456', 'VALUE_457', 'VALUE_458', 'VALUE_465', 'VALUE_466', 'VALUE_468',
              'VALUE_469', 'VALUE_474', 'VALUE_480', 'VALUE_483', 'VALUE_484', 'VALUE_488', 'VALUE_492', 'VALUE_494',
              'VALUE_496', 'VALUE_499', 'VALUE_502', 'VALUE_509', 'VALUE_510', 'VALUE_512', 'VALUE_513', 'VALUE_516',
              'VALUE_517', 'VALUE_523', 'VALUE_524', 'VALUE_530', 'VALUE_531', 'VALUE_534', 'VALUE_536', 'VALUE_540',
              'VALUE_543', 'VALUE_547', 'VALUE_550', 'VALUE_551', 'VALUE_553', 'VALUE_558', 'VALUE_560', 'VALUE_563',
              'VALUE_566', 'VALUE_569', 'VALUE_570', 'VALUE_573', 'VALUE_576', 'VALUE_577', 'VALUE_579', 'VALUE_582',
              'VALUE_589', 'VALUE_590', 'VALUE_591', 'VALUE_593', 'VALUE_595', 'VALUE_597', 'VALUE_600', 'VALUE_602',
              'VALUE_603', 'VALUE_606', 'VALUE_607', 'VALUE_611', 'VALUE_615', 'VALUE_617', 'VALUE_618', 'VALUE_624',
              'VALUE_626', 'VALUE_630', 'VALUE_632', 'VALUE_635', 'VALUE_636', 'VALUE_637', 'VALUE_641', 'VALUE_642',
              'VALUE_644', 'VALUE_646', 'VALUE_647', 'VALUE_648', 'VALUE_655', 'VALUE_657', 'VALUE_658', 'VALUE_660',
              'VALUE_662', 'VALUE_664', 'VALUE_666', 'VALUE_670', 'VALUE_674', 'VALUE_676', 'VALUE_678', 'VALUE_680',
              'VALUE_684', 'VALUE_685', 'VALUE_690', 'VALUE_692', 'VALUE_695', 'VALUE_697', 'VALUE_699', 'VALUE_700',
              'VALUE_702', 'VALUE_706', 'VALUE_708', 'VALUE_711', 'VALUE_713', 'VALUE_715', 'VALUE_720', 'VALUE_721',
              'VALUE_722', 'VALUE_724', 'VALUE_725', 'VALUE_726', 'VALUE_729', 'VALUE_730', 'VALUE_732', 'VALUE_735',
              'VALUE_737', 'VALUE_740', 'VALUE_742', 'VALUE_745', 'VALUE_750', 'VALUE_751', 'VALUE_752', 'VALUE_755',
              'VALUE_757', 'VALUE_758', 'VALUE_759', 'VALUE_763', 'VALUE_764', 'VALUE_766', 'VALUE_768', 'VALUE_771',
              'VALUE_774', 'VALUE_778', 'VALUE_780', 'VALUE_782', 'VALUE_785', 'VALUE_787', 'VALUE_789', 'VALUE_792',
              'VALUE_794', 'VALUE_797', 'VALUE_798', 'VALUE_800', 'VALUE_804', 'VALUE_806', 'VALUE_807', 'VALUE_810',
              'VALUE_812', 'VALUE_814', 'VALUE_816', 'VALUE_818', 'VALUE_819', 'VALUE_823', 'VALUE_825', 'VALUE_827',
              'VALUE_829', 'VALUE_831', 'VALUE_833', 'VALUE_834', 'VALUE_835', 'VALUE_836', 'VALUE_840', 'VALUE_842',
              'VALUE_844', 'VALUE_845', 'VALUE_846', 'VALUE_848', 'VALUE_849', 'VALUE_852', 'VALUE_853', 'VALUE_858',
              'VALUE_859', 'VALUE_863', 'VALUE_865', 'VALUE_870', 'VALUE_872', 'VALUE_873', 'VALUE_874', 'VALUE_876',
              'VALUE_878', 'VALUE_882', 'VALUE_885', 'VALUE_886', 'VALUE_888', 'VALUE_890', 'VALUE_891', 'VALUE_894',
              'VALUE_898', 'VALUE_900', 'VALUE_901', 'VALUE_902', 'VALUE_904', 'VALUE_906', 'VALUE_907', 'VALUE_910',
              'VALUE_912', 'VALUE_913', 'VALUE_914', 'VALUE_915', 'VALUE_917', 'VALUE_918', 'VALUE_920', 'VALUE_924',
              'VALUE_926', 'VALUE_930', 'VALUE_931', 'VALUE_933', 'VALUE_934', 'VALUE_937', 'VALUE_939', 'VALUE_941',
              'VALUE_942', 'VALUE_947', 'VALUE_948', 'VALUE_952', 'VALUE_953', 'VALUE_954', 'VALUE_956', 'VALUE_957',
              'VALUE_958', 'VALUE_960', 'VALUE_961', 'VALUE_964', 'VALUE_966', 'VALUE_967', 'VALUE_968', 'VALUE_969',
              'VALUE_971', 'VALUE_973', 'VALUE_975', 'VALUE_976', 'VALUE_977', 'VALUE_979', 'VALUE_980', 'VALUE_982',
              'VALUE_984', 'VALUE_986', 'VALUE_989', 'VALUE_990', 'VALUE_991', 'VALUE_993', 'VALUE_994', 'VALUE_997',
              'VALUE_998', 'VALUE_999', 'VALUE_1001', 'VALUE_1002', 'VALUE_1005', 'VALUE_1006', 'VALUE_1008',
              'VALUE_1012', 'VALUE_1015', 'VALUE_1018', 'VALUE_1019', 'VALUE_1020', 'VALUE_1021', 'VALUE_1023',
              'VALUE_1025', 'VALUE_1026', 'VALUE_1027', 'VALUE_1030', 'VALUE_1032', 'VALUE_1033', 'VALUE_1034',
              'VALUE_1035', 'VALUE_1036', 'VALUE_1039', 'VALUE_1040', 'VALUE_1041', 'VALUE_1043', 'VALUE_1044',
              'VALUE_1046', 'VALUE_1047', 'VALUE_1049', 'VALUE_1050', 'VALUE_1051', 'VALUE_1053', 'VALUE_1055',
              'VALUE_1056', 'VALUE_1060', 'VALUE_1061', 'VALUE_1063', 'VALUE_1064', 'VALUE_1065', 'VALUE_1068',
              'VALUE_1070', 'VALUE_1072', 'VALUE_1073', 'VALUE_1074', 'VALUE_1075', 'VALUE_1077', 'VALUE_1080',
              'VALUE_1081', 'VALUE_1082', 'VALUE_1083', 'VALUE_1084', 'VALUE_1086', 'VALUE_1087', 'VALUE_1090',
              'VALUE_1092', 'VALUE_1094', 'VALUE_1098', 'VALUE_1100', 'VALUE_1101', 'VALUE_1103', 'VALUE_1106',
              'VALUE_1110', 'VALUE_1111', 'VALUE_1113', 'VALUE_1114', 'VALUE_1116', 'VALUE_1120', 'VALUE_1123',
              'VALUE_1124', 'VALUE_1126', 'VALUE_1127', 'VALUE_1129', 'VALUE_1130', 'VALUE_1132', 'VALUE_1134',
              'VALUE_1135', 'VALUE_1138', 'VALUE_1140', 'VALUE_1141', 'VALUE_1142', 'VALUE_1143', 'VALUE_1145',
              'VALUE_1146', 'VALUE_1148', 'VALUE_1149', 'VALUE_1152', 'VALUE_1154', 'VALUE_1157', 'VALUE_1158',
              'VALUE_1159', 'VALUE_1164', 'VALUE_1166', 'VALUE_1167', 'VALUE_1168', 'VALUE_1170', 'VALUE_1171',
              'VALUE_1173', 'VALUE_1176', 'VALUE_1178', 'VALUE_1179', 'VALUE_1180', 'VALUE_1181', 'VALUE_1182',
              'VALUE_1183', 'VALUE_1186', 'VALUE_1187', 'VALUE_1188', 'VALUE_1189', 'VALUE_1190', 'VALUE_1194',
              'VALUE_1195', 'VALUE_1197', 'VALUE_1198', 'VALUE_1200', 'VALUE_1201', 'VALUE_1203', 'VALUE_1204',
              'VALUE_1205', 'VALUE_1206', 'VALUE_1207', 'VALUE_1209', 'VALUE_1210', 'VALUE_1213', 'VALUE_1214',
              'VALUE_1215', 'VALUE_1218', 'VALUE_1221', 'VALUE_1223', 'VALUE_1224', 'VALUE_1225', 'VALUE_1230',
              'VALUE_1231', 'VALUE_1233', 'VALUE_1234', 'VALUE_1235', 'VALUE_1236', 'VALUE_1239', 'VALUE_1240',
              'VALUE_1242', 'VALUE_1243', 'VALUE_1244', 'VALUE_1247', 'VALUE_1248', 'VALUE_1250', 'VALUE_1251',
              'VALUE_1252', 'VALUE_1253', 'VALUE_1256', 'VALUE_1259', 'VALUE_1260', 'VALUE_1261', 'VALUE_1263',
              'VALUE_1264', 'VALUE_1265', 'VALUE_1266', 'VALUE_1268', 'VALUE_1271', 'VALUE_1272', 'VALUE_1273',
              'VALUE_1274', 'VALUE_1275', 'VALUE_1276', 'VALUE_1277', 'VALUE_1279', 'VALUE_1281', 'VALUE_1282',
              'VALUE_1284', 'VALUE_1288', 'VALUE_1290', 'VALUE_1291', 'VALUE_1292', 'VALUE_1293', 'VALUE_1294',
              'VALUE_1295', 'VALUE_1297', 'VALUE_1298', 'VALUE_1299', 'VALUE_1301', 'VALUE_1302', 'VALUE_1303',
              'VALUE_1306', 'VALUE_1308', 'VALUE_1309', 'VALUE_1310', 'VALUE_1312', 'VALUE_1314', 'VALUE_1315',
              'VALUE_1317', 'VALUE_1318', 'VALUE_1320', 'VALUE_1321', 'VALUE_1323', 'VALUE_1324', 'VALUE_1325',
              'VALUE_1326', 'VALUE_1328', 'VALUE_1331', 'VALUE_1332', 'VALUE_1336', 'VALUE_1337', 'VALUE_1339',
              'VALUE_1340', 'VALUE_1341', 'VALUE_1343', 'VALUE_1344', 'VALUE_1347', 'VALUE_1348', 'VALUE_1350',
              'VALUE_1351', 'VALUE_1352', 'VALUE_1353', 'VALUE_1355', 'VALUE_1356', 'VALUE_1357', 'VALUE_1358',
              'VALUE_1359', 'VALUE_1360', 'VALUE_1361', 'VALUE_1363', 'VALUE_1364', 'VALUE_1366', 'VALUE_1368',
              'VALUE_1369', 'VALUE_1370', 'VALUE_1371', 'VALUE_1373', 'VALUE_1374', 'VALUE_1376', 'VALUE_1379',
              'VALUE_1380', 'VALUE_1381', 'VALUE_1382', 'VALUE_1384', 'VALUE_1385', 'VALUE_1386', 'VALUE_1387',
              'VALUE_1388', 'VALUE_1389', 'VALUE_1391', 'VALUE_1392', 'VALUE_1394', 'VALUE_1395', 'VALUE_1397',
              'VALUE_1398', 'VALUE_1399', 'VALUE_1400', 'VALUE_1402', 'VALUE_1404', 'VALUE_1405', 'VALUE_1406',
              'VALUE_1408', 'VALUE_1410', 'VALUE_1411', 'VALUE_1412', 'VALUE_1413', 'VALUE_1415', 'VALUE_1416',
              'VALUE_1417', 'VALUE_1418', 'VALUE_1421', 'VALUE_1422', 'VALUE_1423', 'VALUE_1425', 'VALUE_1426',
              'VALUE_1429', 'VALUE_1430', 'VALUE_1431', 'VALUE_1432', 'VALUE_1434', 'VALUE_1435', 'VALUE_1436',
              'VALUE_1437', 'VALUE_1440', 'VALUE_1441', 'VALUE_1442', 'VALUE_1443', 'VALUE_1444', 'VALUE_1447',
              'VALUE_1448', 'VALUE_1449', 'VALUE_1451', 'VALUE_1452', 'VALUE_1453', 'VALUE_1455', 'VALUE_1456',
              'VALUE_1458', 'VALUE_1459', 'VALUE_1461', 'VALUE_1462', 'VALUE_1463', 'VALUE_1464', 'VALUE_1465',
              'VALUE_1466', 'VALUE_1467', 'VALUE_1470', 'VALUE_1471', 'VALUE_1472', 'VALUE_1474', 'VALUE_1475',
              'VALUE_1476', 'VALUE_1477', 'VALUE_1480', 'VALUE_1481', 'VALUE_1482', 'VALUE_1484', 'VALUE_1485',
              'VALUE_1487', 'VALUE_1489', 'VALUE_1490', 'VALUE_1491', 'VALUE_1492', 'VALUE_1493', 'VALUE_1494',
              'VALUE_1497', 'VALUE_1499', 'VALUE_1500']

run_aqu = True # set to false if to not use

skip_species = []

# data type dict
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

list_fc = os.listdir(look_up_fc_ab)
list_fc_ab = os.listdir(look_up_fc_ab)
list_dir = os.listdir(in_directory_csv)
file_type = os.path.basename(os.path.dirname(in_directory_csv))
out_path = out_path + os.sep + 'Agg_Layers' + os.sep + file_type
if not os.path.exists(out_path):
    os.mkdir(out_path)


def no_adjust(out_df, final_df, cnty_all, sta_all):
    # Adjust for elevation based on elevation input files

    val_col = [i for i in out_df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_col:
        val_col.remove('VALUE')

    c_df = out_df[['EntityID'] + val_col].copy()
    c_df = c_df.groupby(['EntityID']).sum().reset_index()

    final_df = pd.concat([final_df, c_df])

    w_df = out_df[['EntityID', 'GEOID', 'STATEFP', 'STUSPS'] + val_col].copy()

    df_cnty = w_df.groupby(['EntityID', 'GEOID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    cnty_all = pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state = w_df[['EntityID', 'STATEFP', 'STUSPS'] + val_col].copy()
    df_state = df_state.groupby(['EntityID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    df_state = df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])

    return w_df, final_df, cnty_all, sta_all


def export_aquatics(df, df_final):
    # Export information needed for aquatic tables

    # val_col = ['EntityID', 'HUC2_AB', 'STUSPS', 'STATEFP']
    val_col = ['EntityID', 'HUC2_AB', 'STUSPS']
    [val_col.append(i) for i in df.columns.values.tolist() if str(i).startswith('VALUE')]
    if 'VALUE' in val_col:  # TODO remove in all function once it is cofirmed this col was previous dropped
        val_col.remove('VALUE')

    out_aqu = df[val_col].copy()
    # out_aqu = (out_aqu.groupby(['EntityID', 'HUC2_AB', 'STUSPS', 'STATEFP']).sum()).reset_index()
    out_aqu = (out_aqu.groupby(['EntityID', 'HUC2_AB', 'STUSPS']).sum()).reset_index()
    df_final = pd.concat([df_final, out_aqu])
    return df_final


def split_csv_chucks(in_path, look_up):
    chunksize = 100000
    pp = 0
    j = 1
    out_all_huc = pd.DataFrame(columns=[])
    out_all_other = pd.DataFrame(columns=[])
    look_up.ix[:, 'HUCID'] = look_up.ix[:, 'HUCID'].map(lambda z: str(z).split('.')[0]).astype(str)
    all_zones_sp = look_up['HUCID'].values.tolist()

    # for df in pd.read_csv(in_path, chunksize=chunksize, iterator=True, low_memory=True, dtype=types_dict):
    for df in pd.read_csv(in_path, chunksize=chunksize, iterator=True, low_memory=True):

        # If there is not overlap and table is blank this will add all of the one the fill in the values with 0
        if len(df) == 0:
            df['VALUE'] = all_zones_sp

        df = df.reindex(columns=types_dict.keys())
        df.fillna(0, inplace=True)

    # if len(df) > 0:
        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
        df.index += j
        pp += 1
        value_col = [col for col in df.columns.values.tolist() if col.startswith('VALUE')]
        c_csv = df.copy()
        c_csv['VALUE'] = c_csv['VALUE'].map(lambda k: str(k).split('.')[0]).astype(str)
        list_zones = c_csv['VALUE'].values.tolist()  # list of zones in raw output table
        look_up_huc = look_up[look_up['HUCID'].isin(list_zones)]  # filter lookup to just zones in current output

        # merges working table with HUCID field
        merge_par = pd.merge(c_csv, look_up_huc, how='outer', left_on='VALUE', right_on='HUCID')

        merge_par.to_csv(r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB\test.csv')

        val_cols = [x for x in merge_par.columns.tolist() if str(x).startswith('VALUE')]
        if 'VALUE' in val_cols:
            val_cols.remove('VALUE')

        # merge_par['STATEFP'] = merge_par['GEOID'].map(lambda (n): str(n)[:2] if len(str(n)) == 5 else '0' + n[:1]).astype(str)
        # merged_df_huc = merge_par[['EntityID', 'STUSPS', 'STATEFP', 'HUC2_AB'] + val_cols].copy()
        merged_df_huc = merge_par[['EntityID', 'STUSPS', 'HUC2_AB'] + val_cols].copy()
        # out_sp_table_huc = merged_df_huc.groupby(['EntityID', 'STUSPS', 'STATEFP', 'HUC2_AB'])[
        #     val_cols].sum().reset_index()

        out_sp_table_huc = merged_df_huc.groupby(['EntityID', 'STUSPS', 'HUC2_AB'])[
            val_cols].sum().reset_index()

        merge_par['GEOID'] = merge_par['GEOID'].map(lambda (n): n).astype(str)
        merge_par.ix[:, 'STATEFP'] = merge_par.ix[:, 'GEOID'].map(
            lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

        merged_df_other = merge_par[
            ['EntityID', 'GEOID', 'STUSPS', 'STATEFP'] + val_cols].copy()
        out_sp_table_other = \
            merged_df_other.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP'])[
                val_cols].sum().reset_index()

        if len(out_sp_table_huc) > 0:
            if 'VALUE_0' not in out_sp_table_huc.columns.values.tolist():
                out_sp_table_huc['VALUE_0'] = 0
            out_sp_table_huc.ix[:, ["VALUE_0"]] = out_sp_table_huc.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
            # print len (out_sp_table_huc)
        if len(out_sp_table_other) > 0:
            if 'VALUE_0' not in out_sp_table_other.columns.values.tolist():
                out_sp_table_other['VALUE_0'] = 0
            out_sp_table_other.ix[:, ["VALUE_0"]] = out_sp_table_other.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
            # print len (out_sp_table_other)

            out_sp_table_other['GEOID'] = out_sp_table_other['GEOID'].map(lambda (n): n).astype(str)
            out_sp_table_other['STATEFP'] = out_sp_table_other['GEOID'].map(
                lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
            out_all_huc = pd.concat([out_all_huc, out_sp_table_huc])
            out_all_other = pd.concat([out_all_other, out_sp_table_other])
            j = df.index[-1] + 1

            print('Finish part {0} of table'.format(pp))
        else:
            print '    ****Check overlap tables for above run, no row confirm this is correct***'

    return out_all_huc, out_all_other
    # except:
    #     print 'Failed on {0}'.format(csv)
    #     return pd.DataFrame(columns=[]),pd.DataFrame(columns=[]),pd.DataFrame(columns=[])


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

if not os.path.exists(os.path.dirname(out_path)):
    os.mkdir(os.path.dirname(out_path))

if not os.path.exists(out_path):
    os.mkdir(out_path)
for folder in list_dir:
    print folder
    # Use when we do not want to overwrite the file that were previously generatate
    # if os.path.exists(out_path + os.sep + folder):
    #     print 'Already created tables for {0}'.format(folder)
    #     continue
    out_folder = out_path
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

    list_csv = os.listdir(in_directory_csv + os.sep + folder)  # list of csv in folder
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]  # list of att csvs
    # loops on each csv added the HUCIDs and ZoneIDs from parent fc att table to working dfs, then transforms table
    # so it is entityID by elevation or habitat
    for csv in list_csv:
        if csv.split("_")[0] + "_" + csv.split("_")[1] in skip_species:
            continue
        # elif os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')):
        #     print out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')
        #     # continue
        else:
            if not os.path.exists(out_pol_folder_st + os.sep + csv):

                print ("   Working on {0}...table {1} of {2}".format(csv, (list_csv.index(csv) + 1), len(list_csv)))
                # parent fc att table with all input ID field (list_fc_ab) and ZoneID and associated EntityID (list_fc)
                # for species listed in the csv title
                lookup_csv = [t for t in list_fc_ab if t.startswith(csv.split("_")[0].upper()
                                                                    + "_" + csv.split("_")[1].capitalize())]

                lookup_df = pd.read_csv(look_up_fc_ab + os.sep + lookup_csv[0], dtype=object)

                in_csv_path = in_directory_csv + os.sep + folder + os.sep + csv

                # out_all, out_all_cnty, out_all_state = split_csv_chucks (in_csv_path)
                out_all_huc, out_all_other = split_csv_chucks(in_csv_path, lookup_df)
                # SET COL ORDER; COL WITH NO OVERLAP NOT FOUND IN ORIGINAL OUTPUT TABLE WILL BE ADD AND NEED
                # TO BE SET TO 0
                out_all_huc = out_all_huc.reindex(columns= final_cols)
                out_all_other = out_all_other.reindex(columns= final_cols_all)
                out_all_huc.fillna(0, inplace=True)  # FILLS IN EMPTY COLS WITH 0
                out_all_other.fillna(0, inplace=True)  # FILLS IN EMPTY COLS WITH 0


                # print len(out_all_other), len (out_all_huc)

                # Generates very large files run transforms and summarize to species in memory to keep file size low
                # out_all.to_csv(out_folder + os.sep + csv)

                aqu = pd.DataFrame(columns=[])

                no_adjust_no = pd.DataFrame(columns=[])
                no_adjust_cnty = pd.DataFrame(columns=[])
                no_adjust_state = pd.DataFrame(columns=[])
                if len(out_all_huc) > 0 and len(out_all_other) > 0:

                    loop_sp_table = out_all_huc.copy()
                    if not os.path.exists(out_folder + os.sep + folder):
                        os.mkdir(out_folder + os.sep + folder)
                    if run_aqu:
                        if not os.path.exists(
                                out_folder + os.sep + folder + os.sep + csv.replace('.csv', '_HUC2AB.csv')):

                            aqu = export_aquatics(loop_sp_table, aqu)
                            aqu.to_csv(out_folder + os.sep + folder + os.sep + csv.replace('.csv', '_HUC2AB.csv'))
                            print '  Exported {0}'.format(
                                out_folder + os.sep + folder + os.sep + csv.replace('.csv', '_HUC2AB.csv'))

                    loop_sp_table = out_all_other.copy()
                    if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')):
                        no_adjust_working, no_adjust_no, no_adjust_cnty, no_adjust_state = no_adjust(loop_sp_table,
                                                                                                     no_adjust_no,
                                                                                                     no_adjust_cnty,
                                                                                                     no_adjust_state)
                        no_adjust_no.to_csv(
                            out_folder + os.sep + folder + os.sep + csv.replace('.csv',
                                                                                '_noadjust.csv'))
                        no_adjust_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_noadjust.csv'))
                        no_adjust_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))
                        print '  Exported {0}'.format(
                            out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))



            else:
                print ('Already created all tables, the last table is: {0}'.format((out_pol_folder_st + os.sep + csv)))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

