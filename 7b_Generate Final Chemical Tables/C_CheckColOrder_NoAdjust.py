import pandas as pd
import os


def main():

    path = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Carbaryl\Critical Habitat\ParentTables\no adjustment'
    # path = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\Range\SprayInterval_IntStep_30_MaxDistance_1501\ParentTables\no adjustment'
    final_order = ['Unnamed: 0', 'EntityID', 'GEOID','STATEFP', 'STUSPS','VALUE_0', 'VALUE_30',
                   'VALUE_42', 'VALUE_60','VALUE_67', 'VALUE_84', 'VALUE_90', 'VALUE_94', 'VALUE_108', 'VALUE_120',
                   'VALUE_123', 'VALUE_127', 'VALUE_134', 'VALUE_150', 'VALUE_152', 'VALUE_161', 'VALUE_169',
                   'VALUE_174', 'VALUE_180',
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
    list_files = recursive_file_lookup(path)
    check_order(list_files, final_order)


def recursive_file_lookup(path):
    files_list = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if os.path.join(root, name).endswith('.csv'):
                files_list.append(os.path.join(root, name))
        # for name in dirs:
        #     print(os.path.join(root, name))
    return files_list


def check_order(list_csvs, order_col):
    for csv in list_csvs:
        # print csv
        df = pd.read_csv(csv)
        if "Unnamed: 0.1" in df.columns.values.tolist():
            df.drop("Unnamed: 0.1", axis= "columns", inplace= True)
        current_cols = df.columns.values.tolist()
        dict_order = {}
        for i in order_col:
            index_value = order_col.index(i)
            dict_order[i] = index_value
        reindex = False
        for col in current_cols:
            try:
                index_check = current_cols.index(col)
                final_index = dict_order[col]
                if final_index == index_check:
                    pass
                else:
                    print('Columns are out of order')
                    # print current_cols
                    # print order_col
                    reindex = True
                    break
            except KeyError:
                print'Additional col'
        missing_cols = [col for col in order_col if col not in current_cols]
        extra_cols = [col for col in current_cols  if col not in order_col]
        if len(missing_cols) >0:
            print('Table {0} is missing columns {1}'.format(csv, missing_cols))
        if len(extra_cols) >0:
            print('Table {0} has extra columns {1}'.format(csv, extra_cols))
        if reindex:
            df= df.reindex(columns = order_col)
        df.to_csv(csv)


main()
