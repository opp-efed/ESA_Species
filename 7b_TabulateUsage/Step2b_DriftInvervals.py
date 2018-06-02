import pandas as pd
import os
import datetime
import numpy as np

# TODO Clean up on/off field - move to functions
# TODO FILTER NE/NLAAs

chemical_name = 'Carbaryl'
chemical_name = 'Methomyl'
use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
             r'\SupportingTables' + os.sep + chemical_name + "_Uses_lookup_20180430.csv"


max_drift = '765'
l48_BE_interval = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                  r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
                  r'\R_SprayInterval_20180522_Region.csv'

nl48_BE_interval = 'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                   '\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
                   '\R_SprayInterval_20180522_NL48Range.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

out_location = 'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
               '\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables'

find_file_type = os.path.basename(l48_BE_interval)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'

on_off_cult_species = ['4', '5', '9', '17', '21', '28', '29', '31', '32', '33', '34', '35', '37', '38', '39', '40',
                       '41', '42', '43', '46', '49', '50', '51', '53', '54', '55', '56', '57', '58', '59', '60', '61',
                       '62', '63', '65', '74', '79', '80', '81', '84', '85', '86', '87', '94', '95', '96', '97', '98',
                       '99', '102', '103', '107', '112', '113', '115', '116', '120', '121', '126', '127', '128', '129',
                       '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '142', '145', '149',
                       '150', '151', '152', '156', '162', '163', '164', '166', '167', '168', '169', '170', '171', '172',
                       '174', '175', '176', '177', '178', '179', '180', '182', '183', '185', '192', '196', '198', '200',
                       '387', '389', '390', '391', '392', '393', '394', '395', '419', '420', '421', '423', '424', '425',
                       '426', '427', '428', '429', '430', '431', '432', '433', '434', '437', '438', '443', '444', '446',
                       '450', '451', '452', '455', '456', '457', '458', '462', '496', '497', '498', '500', '501', '502',
                       '503', '504', '505', '506', '507', '508', '509', '510', '511', '512', '513', '514', '515', '516',
                       '518', '519', '520', '521', '523', '524', '525', '526', '528', '529', '530', '531', '532', '533',
                       '535', '536', '537', '539', '540', '541', '542', '543', '544', '545', '546', '547', '548', '549',
                       '550', '551', '552', '553', '554', '555', '556', '557', '558', '559', '560', '561', '562', '563',
                       '564', '565', '566', '567', '569', '570', '571', '572', '573', '574', '575', '576', '577', '578',
                       '579', '581', '584', '585', '586', '587', '588', '589', '590', '591', '592', '593', '594', '595',
                       '596', '597', '598', '599', '600', '601', '602', '603', '604', '605', '607', '608', '609', '610',
                       '611', '612', '613', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '624',
                       '626', '627', '628', '629', '630', '631', '632', '633', '634', '635', '636', '637', '638', '639',
                       '640', '641', '642', '643', '644', '645', '646', '647', '648', '649', '650', '651', '652', '653',
                       '654', '655', '656', '657', '658', '659', '660', '661', '662', '663', '664', '665', '666', '667',
                       '668', '669', '670', '671', '672', '673', '674', '675', '676', '679', '680', '681', '682', '683',
                       '684', '685', '686', '687', '688', '689', '690', '691', '692', '693', '694', '695', '696', '698',
                       '700', '701', '702', '703', '704', '705', '707', '708', '709', '710', '712', '713', '715', '716',
                       '717', '718', '719', '720', '721', '722', '723', '724', '725', '726', '727', '728', '729', '730',
                       '731', '732', '733', '734', '735', '736', '737', '738', '739', '740', '741', '742', '743', '744',
                       '745', '746', '747', '748', '749', '750', '751', '752', '753', '754', '755', '756', '757', '758',
                       '759', '760', '761', '762', '763', '764', '765', '766', '767', '768', '769', '770', '771', '772',
                       '773', '774', '775', '776', '777', '778', '779', '780', '781', '782', '783', '784', '788', '789',
                       '790', '791', '792', '793', '794', '795', '796', '797', '798', '800', '801', '802', '803', '804',
                       '805', '806', '808', '809', '810', '811', '812', '813', '814', '815', '816', '817', '818', '819',
                       '820', '821', '822', '824', '825', '826', '827', '828', '829', '830', '831', '832', '833', '835',
                       '836', '837', '838', '839', '840', '841', '842', '843', '844', '845', '846', '847', '848', '849',
                       '850', '851', '852', '853', '854', '855', '856', '857', '859', '860', '861', '862', '863', '864',
                       '865', '866', '867', '868', '869', '871', '872', '873', '874', '876', '878', '879', '880', '881',
                       '882', '883', '884', '885', '886', '887', '888', '889', '890', '891', '892', '893', '894', '895',
                       '896', '897', '898', '899', '900', '901', '902', '903', '904', '905', '906', '907', '908', '909',
                       '910', '911', '912', '913', '914', '915', '916', '917', '919', '920', '921', '922', '923', '924',
                       '925', '926', '927', '928', '929', '930', '932', '933', '934', '935', '936', '937', '938', '939',
                       '940', '941', '942', '943', '945', '946', '947', '948', '949', '950', '951', '952', '953', '954',
                       '955', '956', '957', '958', '959', '960', '961', '962', '963', '964', '965', '966', '967', '968',
                       '969', '970', '971', '972', '973', '974', '975', '976', '977', '978', '979', '980', '981', '982',
                       '983', '984', '985', '986', '987', '988', '989', '990', '991', '992', '993', '994', '995', '996',
                       '997', '998', '999', '1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008',
                       '1009', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', '1018', '1019', '1020',
                       '1021', '1022', '1023', '1024', '1025', '1026', '1027', '1029', '1030', '1031', '1032', '1033',
                       '1034', '1035', '1036', '1037', '1038', '1039', '1040', '1041', '1042', '1043', '1044', '1045',
                       '1046', '1048', '1049', '1050', '1051', '1052', '1053', '1054', '1055', '1056', '1057', '1058',
                       '1059', '1061', '1062', '1063', '1065', '1066', '1067', '1068', '1069', '1070', '1071', '1072',
                       '1073', '1074', '1075', '1076', '1077', '1078', '1079', '1080', '1081', '1082', '1083', '1084',
                       '1085', '1086', '1087', '1088', '1089', '1090', '1091', '1092', '1093', '1094', '1095', '1096',
                       '1097', '1098', '1099', '1101', '1103', '1104', '1105', '1106', '1107', '1108', '1109', '1110',
                       '1111', '1112', '1113', '1114', '1115', '1116', '1117', '1118', '1119', '1120', '1121', '1122',
                       '1123', '1124', '1125', '1126', '1127', '1128', '1129', '1130', '1131', '1132', '1133', '1134',
                       '1135', '1136', '1137', '1138', '1139', '1140', '1141', '1143', '1144', '1145', '1146', '1147',
                       '1148', '1149', '1150', '1151', '1152', '1153', '1154', '1155', '1156', '1157', '1158', '1159',
                       '1160', '1162', '1163', '1164', '1165', '1166', '1167', '1168', '1169', '1170', '1171', '1172',
                       '1173', '1174', '1175', '1176', '1177', '1178', '1179', '1180', '1181', '1182', '1183', '1184',
                       '1185', '1186', '1188', '1189', '1190', '1191', '1192', '1193', '1194', '1195', '1196', '1197',
                       '1198', '1200', '1201', '1202', '1205', '1206', '1207', '1208', '1209', '1210', '1211', '1212',
                       '1213', '1214', '1215', '1216', '1217', '1218', '1219', '1220', '1221', '1222', '1223', '1224',
                       '1225', '1226', '1227', '1229', '1230', '1231', '1232', '1233', '1234', '1235', '1241', '1250',
                       '1252', '1253', '1254', '1255', '1256', '1258', '1259', '1262', '1263', '1264', '1265', '1266',
                       '1267', '1278', '1283', '1311', '1349', '1378', '1400', '1407', '1415', '1497', '1502', '1521',
                       '1525', '1535', '1607', '1609', '1623', '1632', '1636', '1645', '1678', '1693', '1709', '1710',
                       '1760', '1783', '1831', '1840', '1862', '1881', '1935', '1968', '1984', '1989', '2036', '2085',
                       '2118', '2154', '2211', '2265', '2268', '2273', '2278', '2364', '2404', '2458', '2517', '2567',
                       '2619', '2682', '2683', '2727', '2730', '2758', '2778', '2780', '2782', '2810', '2823', '2860',
                       '2934', '2970', '3020', '3049', '3054', '3084', '3116', '3154', '3175', '3194', '3224', '3267',
                       '3271', '3292', '3385', '3387', '3388', '3412', '3472', '3540', '3592', '3653', '3670', '3671',
                       '3722', '3728', '3737', '3753', '3784', '3832', '3849', '3871', '3876', '3990', '3999', '4000',
                       '4007', '4030', '4136', '4179', '4201', '4228', '4237', '4238', '4253', '4297', '4308', '4377',
                       '4395', '4413', '4420', '4487', '4508', '4533', '4551', '4564', '4565', '4589', '4630', '4680',
                       '4712', '4724', '4740', '4754', '4858', '4889', '4910', '5067', '5168', '5170', '5186', '5210',
                       '5233', '5273', '5334', '5468', '5580', '5610', '5709', '5763', '5956', '6019', '6097', '6176',
                       '6257', '6303', '6490', '6522', '6536', '6617', '6620', '6632', '6672', '6679', '6782', '6845',
                       '6870', '6901', '6969', '7046', '7054', '7067', '7116', '7136', '7167', '7170', '7206', '7220',
                       '7229', '7254', '7261', '7280', '7367', '7495', '7529', '7572', '7617', '7731', '7805', '7840',
                       '7886', '7892', '7907', '7918', '7948', '7979', '7992', '8083', '8166', '8254', '8277', '8303',
                       '8336', '8338', '8347', '8357', '8392', '8503', '8621', '8683', '8684', '8685', '8962', '9001',
                       '9338', '9721', '9725', '9929', '9951', '9952', '9953', '9954', '9955', '9956', '9957', '9958',
                       '9959', '9960', '9961', '9962', '9963', '9965', '10007', '10008', '10009', '10021', '10028',
                       '10034', '10043', '10076', '10147', '10161', '10178', '10222', '10223', '10224', '10225',
                       '10227', '10228', '10229', '10231', '10232', '10233', '10234', '10235', '10290', '10479',
                       '10480', '10481', '10483', '10583', '10584', '10585', '10586', '10587', '10588', '10590',
                       '10591', '10593', '10594', '10599', '10719', '10720', '10721', '10722', '10723', '10724',
                       '10725', '10726', '10727', '10728', '10729', '10732', '10909', '11260', '11340']

on_off_pasture_species = ['4', '8', '13', '28', '29', '31', '32', '34', '35', '41', '42', '43', '46', '49', '50', '53',
                          '54', '58', '60', '62', '63', '65', '74', '79', '80', '81', '84', '85', '86', '87', '94',
                          '95', '97', '98', '99', '102', '103', '111', '112', '113', '116', '118', '120', '121', '123',
                          '124', '127', '128', '129', '130', '131', '132', '134', '135', '136', '137', '139', '140',
                          '142', '145', '150', '151', '152', '156', '162', '163', '167', '168', '169', '170', '171',
                          '172', '174', '175', '176', '177', '178', '179', '180', '185', '192', '196', '198', '200',
                          '387', '389', '390', '391', '392', '393', '394', '395', '419', '421', '424', '426', '429',
                          '432', '434', '437', '443', '446', '452', '455', '456', '457', '458', '497', '511', '515',
                          '521', '530', '531', '533', '536', '537', '545', '548', '556', '558', '560', '561', '563',
                          '564', '565', '570', '573', '575', '581', '584', '589', '590', '591', '597', '598', '602',
                          '604', '613', '616', '618', '621', '622', '623', '628', '630', '634', '637', '641', '644',
                          '648', '650', '654', '659', '664', '672', '673', '674', '676', '685', '686', '688', '691',
                          '692', '693', '696', '697', '700', '712', '713', '715', '717', '719', '720', '721', '722',
                          '725', '726', '727', '731', '732', '734', '735', '736', '737', '738', '741', '742', '745',
                          '746', '747', '755', '759', '764', '765', '766', '767', '769', '771', '772', '773', '774',
                          '775', '778', '779', '780', '781', '782', '800', '801', '810', '812', '814', '815', '817',
                          '818', '819', '820', '822', '827', '829', '831', '832', '833', '836', '838', '839', '843',
                          '844', '845', '846', '850', '851', '852', '857', '860', '861', '863', '864', '865', '866',
                          '869', '871', '874', '879', '880', '881', '882', '884', '886', '889', '890', '892', '893',
                          '894', '895', '896', '900', '903', '904', '905', '906', '908', '909', '912', '915', '916',
                          '917', '919', '921', '924', '925', '926', '933', '936', '937', '938', '942', '943', '946',
                          '947', '948', '950', '951', '953', '954', '955', '956', '958', '959', '960', '962', '963',
                          '964', '965', '966', '968', '970', '971', '973', '976', '978', '980', '981', '982', '983',
                          '985', '986', '987', '992', '993', '995', '998', '1002', '1004', '1006', '1007', '1012',
                          '1014', '1015', '1016', '1017', '1018', '1020', '1021', '1023', '1026', '1027', '1029',
                          '1038', '1040', '1042', '1044', '1049', '1050', '1052', '1055', '1057', '1062', '1063',
                          '1067', '1069', '1070', '1071', '1072', '1075', '1077', '1084', '1091', '1092', '1093',
                          '1094', '1097', '1098', '1099', '1101', '1103', '1104', '1105', '1106', '1107', '1108',
                          '1109', '1110', '1111', '1112', '1113', '1114', '1116', '1117', '1120', '1121', '1122',
                          '1124', '1127', '1129', '1130', '1131', '1132', '1133', '1136', '1137', '1138', '1139',
                          '1141', '1144', '1147', '1150', '1151', '1152', '1153', '1155', '1158', '1159', '1160',
                          '1162', '1163', '1164', '1168', '1169', '1171', '1175', '1176', '1177', '1178', '1179',
                          '1180', '1181', '1184', '1185', '1186', '1188', '1189', '1190', '1191', '1192', '1193',
                          '1194', '1195', '1196', '1197', '1198', '1200', '1201', '1202', '1205', '1206', '1207',
                          '1208', '1209', '1210', '1211', '1212', '1213', '1214', '1215', '1216', '1217', '1218',
                          '1221', '1224', '1226', '1228', '1230', '1231', '1232', '1241', '1250', '1252', '1253',
                          '1254', '1255', '1256', '1258', '1259', '1264', '1265', '1278', '1311', '1502', '1521',
                          '1525', '1607', '1623', '1645', '1678', '1693', '1709', '1760', '1783', '1840', '1862',
                          '1935', '1984', '2036', '2118', '2211', '2268', '2273', '2278', '2364', '2404', '2619',
                          '2682', '2727', '2778', '2782', '3020', '3049', '3116', '3154', '3267', '3271', '3292',
                          '3387', '3388', '3472', '3592', '3671', '3722', '3728', '3753', '3784', '3871', '3990',
                          '3999', '4000', '4136', '4237', '4297', '4308', '4377', '4413', '4508', '4551', '4589',
                          '4680', '4754', '4858', '4889', '4910', '5067', '5168', '5170', '5186', '5580', '5763',
                          '6097', '6176', '6257', '6522', '6536', '6617', '6620', '6672', '6679', '6901', '7054',
                          '7067', '7116', '7167', '7229', '7254', '7261', '7367', '7529', '7840', '7886', '7907',
                          '7918', '8083', '8166', '8277', '8336', '8357', '8621', '8962', '9001', '9721', '9929',
                          '9952', '9953', '9954', '9955', '9956', '9957', '9958', '9959', '9960', '9961', '9962',
                          '9963', '10007', '10008', '10009', '10021', '10043', '10178', '10232', '10583', '10584',
                          '10585', '10586', '10587', '10593', '10594', '10599', '10719', '10720', '10721', '10723',
                          '10724', '10726', '10727', '10728', '10729', '10732', '10909', '11340', ]
on_off_orchards_species = []

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def on_off_field(row, cols, df, on_off_species):
    ent_id = str(row['EntityID'])
    col = [v for v in cols if v.endswith("_0")]
    if ent_id in on_off_species:
        for i in col:
            df.loc[df['EntityID'] == ent_id, i] = 0


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# set up directory
create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name

# load input files
use_lookup_df = pd.read_csv(use_lookup, dtype=object)
l48_df = pd.read_csv(l48_BE_interval, dtype=object)
nl48_df = pd.read_csv(nl48_BE_interval,dtype=object)

# ser entity id to str
l48_df.ix[:, 'EntityID'] = l48_df['EntityID'].map(lambda x: x).astype(str)
l48_df.ix[:, 'EntityID'] = l48_df['EntityID'].map(lambda x: x).astype(str)

# Extract columns that will be adjusted for redundancy from input tables
other_layer = use_lookup_df.loc[(use_lookup_df['other layer'] == 'x')]
other_layer_cols = other_layer['Chem Table FinalColHeader'].values.tolist()
ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA Ag'] == 'x')]
ag_cols = ag_headers['Chem Table FinalColHeader'].values.tolist()

# Try except loop to set a boolean variable to skip non ag adjustment if there are no non ag uses
try:
    non_ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA NonAg'] == 'x')]
    nonag_cols = non_ag_headers['Chem Table FinalColHeader'].values.tolist()
    if len(non_ag_headers) == 0:
        skip_non_ag_adjustment = True
    else:
        skip_non_ag_adjustment = False
except TypeError:
    non_ag_headers =[]
    nonag_cols =[]
    skip_non_ag_adjustment = True

# ## Extract columns that will be adjusted for the on/off field
on_off_cult = use_lookup_df.loc[(use_lookup_df['On/Off_AG'] == 'x')]
on_off_pasture = use_lookup_df.loc[(use_lookup_df['On/Off_Pasture'] == 'x')]
# on_off_orchard = use_lookup_df.loc[(use_lookup_df['On/Off_Orchard_Plantation'] == 'x')]

on_off_cult_cols = on_off_cult['FinalColHeader'].values.tolist()
on_off_pasture_cols = on_off_pasture['FinalColHeader'].values.tolist()
# on_off_orchard_cols = on_off_orchard['FinalColHeader'].values.tolist()

on_off_cult_cols = [x if x.split("_")[0] == 'CONUS' else 'NL48_' + x.split("_")[1] for x in on_off_cult_cols]
on_off_cult_cols = list(set(on_off_cult_cols))

on_off_pasture_cols = on_off_pasture['FinalColHeader'].values.tolist()
on_off_pasture_cols = [x if x.split("_")[0] == 'CONUS' else 'NL48_' + x.split("_")[1] for x in on_off_pasture_cols]
on_off_pasture_cols = list(set(on_off_pasture_cols))

collapsed_dict = {}

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df.ix[:, 'EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)


# STEP 1 :     ##Filter L48 AA
out_path_no_adjustment = out_path + os.sep + 'No Adjustment'
create_directory(out_path_no_adjustment)

# Extact column head that are need for chemical based on input file
aa_layers_CONUS = use_lookup_df.loc[(use_lookup_df['Included AA'] == 'x') & (use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

# trandorm input table for nl48 so it can be filtered  by use and set header - remove index row
df_t = l48_df.T.reset_index()
new_header = df_t.iloc[1]
df_t.columns= new_header

# add column that extract the col_prefix from use name with interval that is in the first column after the
# transformation, header is Entity ID
df_t ['FinalColHeader'] = df_t['EntityID'].map(lambda x: x.split("_")[0]+"_"+x.split("_")[1] if len (x.split("_")) >1 else 0).astype(str)
# filter table to just those col_prefix needed for chemical
filter_df_t = df_t[(df_t['FinalColHeader'].isin(col_prefix_CONUS))]
# filter species info from table
species_info = df_t.iloc[0:5]
# concats the two filtered tables
m_df = pd.concat([species_info, filter_df_t ], axis =0).reset_index(drop=True)
m_df.drop('FinalColHeader', axis=1, inplace=True)
# transforms tables so it has the use intervals for columns and species as rows again, set col header, remove old index
# rows and drops extra columns
aa_l48 = m_df.T
new_header = aa_l48.iloc[0]
aa_l48.columns= new_header
aa_l48 = aa_l48.iloc[1:]
aa_l48['EntityID'] = aa_l48.index
aa_l48 = aa_l48.reset_index(drop=True)
[aa_l48.drop(m, axis=1, inplace=True) for m in aa_l48.columns.values.tolist() if not m.startswith('CONUS') and m != 'EntityID']

# list of columns need for the conus tables - used in other steps
conus_cols = aa_l48.columns.values.tolist()

# merges table to base species informations and exports to csv
out_aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
out_aa_l48.to_csv(out_path_no_adjustment + os.sep + file_type + 'CONUS_Step2_Intervals_' + chemical_name + '.csv')

# STEP 1B  : # ##Filter NL48 AA
# Extact column head that are need for chemical based on input file
aa_layers_NL48 = use_lookup_df.loc[(use_lookup_df['Included AA'] == 'x') & (use_lookup_df['Region'] != 'CONUS')]
col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()
col_prefix_NL48_noregion = [x if x.split("_")[0] == 'CONUS' else 'NL48_' + x.split("_")[1] for x in col_prefix_NL48]
cols_all_uses_nl48 = nl48_df.columns.values.tolist()

# generate the list cols needed for the NL48 - TODO update to the transoform and filter done for CONUS
cols_aa_nl48 = []

for v in col_prefix_NL48:
    for col in cols_all_uses_nl48:
        if col.startswith(v):
            cols_aa_nl48.append(col)
        else:
            pass
cols_aa_nl48.insert(0, 'EntityID')

# sums the values from each individual region into the the value for combined NL48 region based on common use intervals
# ie AK _0 + AS_0 + HI_0 ...
intervals = []
for i in cols_aa_nl48:
    int_bin = i.split("_")[len(i.split("_")) - 1]
    if int_bin not in intervals and int_bin != 'EntityID':
        intervals.append(int_bin)

for t in intervals:
    binned_intervals = [x for x in cols_aa_nl48 if x.endswith("_" + t)]
    binned_uses_list = [x.split("_")[1] for x in binned_intervals]
    binned_uses_list = list(set(binned_uses_list))

for z in binned_uses_list:
    for t in intervals:
        binned_use = [p for p in binned_intervals if p.split("_")[1] == z]
        out_nl48_col = 'NL48_' + z + "_" + t
        nl48_df.ix[:,binned_use] = nl48_df.ix[:,binned_use].apply(pd.to_numeric, errors='coerce')
        nl48_df[out_nl48_col] = nl48_df[binned_use].sum(axis=1)
# list of columns need for the nl48 tables - used to filter here and in other steps
out_cols = nl48_df.columns.values.tolist()
nl48_cols_f = [k for k in out_cols if k.startswith('NL48')]
nl48_cols_f.insert(0, 'EntityID')

# filter to just the cols needed for  nl48
aa_nl48 = nl48_df.ix[:, nl48_cols_f]

# merges table to base species informations and exports to csv
out_aa_nl48 = pd.merge(base_sp_df, aa_nl48, on='EntityID', how='left')
out_aa_nl48.to_csv(out_path_no_adjustment + os.sep + file_type + 'NL48_Step2_Intervals_' + chemical_name + '.csv')

# STEP 2  : Adjusted for redundancy
# redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite factors that are calculated
# Merges nl48 and conus into one df so that routine is the same as the 2a_Tables_overlap
#  TODO move this routine to a function or class object so both can call the same code
chemical_step1 = pd.merge(aa_l48, aa_nl48, on='EntityID', how='left')
# ## set up out path
out_path_redundancy = out_path+ os.sep + 'Redundancy_only'
create_directory(out_path_redundancy)
chemical_step1.drop_duplicates()
# Set up the different list of columns that apply to conus vs nl48, aa, composites, use, ag and non-ag
# conus/nonl48
overlap_cols_conus = [x for x in conus_cols if x not in col_include_output]
overlap_cols_nl48 = [x for x in nl48_cols_f if x not in col_include_output]
# layer flag as other that should not be adjusted ie Federal Lands
other_layer_header  =[x for x in other_layer_cols]

# AA and composite columnms - HARD CODE composite and actions areas must have AA in the use name
aa_col_conus = [x for x in overlap_cols_conus if 'AA' in x.split('_')[1].split(' ')]
aa_col_nl48 = [x for x in overlap_cols_nl48  if 'AA' in x.split('_')[1].split(' ')]

# Use columns
use_col_conus = [x for x in overlap_cols_conus if not 'AA' in x.split('_')[1].split(' ') and (x.split('_')[0]+"_"+x.split('_')[1])  not in other_layer_cols]
use_col_nl48 = [x for x in overlap_cols_nl48  if not 'AA' in x.split('_')[1].split(' ')and (x.split('_')[0]+"_"+x.split('_')[1]) not in other_layer_cols]

# filters list so only the direct overlap is in list (represent by an _0 at the end of the column header
use_direct_only_conus_ag = [x for x in use_col_conus if x.endswith('_0') and (x.split('_')[0]+"_"+x.split('_')[1])   in ag_cols]
use_direct_only_conus_nonag = [x for x in use_col_conus if x.endswith('_0') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]
use_direct_only_nl48_ag  = [x for x in use_col_nl48  if x.endswith('_0')and (x.split('_')[0]+"_"+x.split('_')[1])  in ag_cols]
use_direct_only_nl48_nonag  = [x for x in use_col_nl48  if x.endswith('_0') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]

use_direct_only_conus_ag_aa = [x for x in aa_col_conus if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
use_direct_only_conus_nonag_aa = [x for x in aa_col_conus if x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
use_direct_only_nl48_ag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
use_direct_only_nl48_nonag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
use_direct_only_conus_aa = [x for x in aa_col_conus if x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ")and 'NonAg' not in x.split("_")[1].split(" ")]
use_direct_only_nl48_aa  = [x for x in aa_col_nl48   if x.endswith('_0')and 'Ag' not in x.split("_")[1].split(" ")and 'NonAg' not in x.split("_")[1].split(" ")]

# filters list so only the ground and aerial overlap is in list (represent by an _305 and _765 at the end of the column header
use_direct_only_conus_ag_ground = [x for x in use_col_conus if x.endswith('_305') and (x.split('_')[0]+"_"+x.split('_')[1])   in ag_cols]
use_direct_only_conus_nonag_ground = [x for x in use_col_conus if x.endswith('_305') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]
use_direct_only_nl48_ag_ground  = [x for x in use_col_nl48  if x.endswith('_305')and (x.split('_')[0]+"_"+x.split('_')[1])  in ag_cols]
use_direct_only_nl48_nonag_ground  = [x for x in use_col_nl48  if x.endswith('_305') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]

use_direct_only_conus_ag_aerial = [x for x in use_col_conus if x.endswith('_765') and (x.split('_')[0]+"_"+x.split('_')[1])   in ag_cols]
use_direct_only_conus_nonag_aerial = [x for x in use_col_conus if x.endswith('_765') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]
use_direct_only_nl48_ag_aerial = [x for x in use_col_nl48  if x.endswith('_765')and (x.split('_')[0]+"_"+x.split('_')[1])  in ag_cols]
use_direct_only_nl48_nonag_aerial = [x for x in use_col_nl48  if x.endswith('_765') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]


# Confirms all number are set to a numeric data type
chemical_step1.ix[:, use_direct_only_conus_ag] = chemical_step1.ix[:,use_direct_only_conus_ag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_ag] = chemical_step1.ix[:,use_direct_only_nl48_ag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa] = chemical_step1.ix[:, use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa] = chemical_step1.ix[:, use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa].apply(pd.to_numeric, errors='coerce')
if not skip_non_ag_adjustment:
    chemical_step1.ix[:,use_direct_only_conus_aa[0]] = chemical_step1.ix[:,use_direct_only_conus_aa[0]].apply(pd.to_numeric, errors='coerce')
    chemical_step1.ix[:,use_direct_only_nl48_aa[0]] = chemical_step1.ix[:,use_direct_only_nl48_aa[0]].apply(pd.to_numeric, errors='coerce')

# Sum uses in the three groups ag, non ag and composites for both CONUS adn NL48
chemical_step1['CONUS_Sum_Ag'] = chemical_step1[use_direct_only_conus_ag].sum(axis=1)
chemical_step1['CONUS_Sum_NonAg'] = chemical_step1[use_direct_only_conus_nonag].sum(axis=1)
chemical_step1['NL48_Sum_Ag'] = chemical_step1[use_direct_only_nl48_ag].sum(axis=1)
chemical_step1['NL48_Sum_NonAg'] = chemical_step1[use_direct_only_nl48_nonag].sum(axis=1)

chemical_step1['CONUS_Sum_Composites'] = chemical_step1[use_direct_only_conus_ag_aa +use_direct_only_conus_nonag_aa].sum(axis=1)
chemical_step1['NL48_Sum_Composites'] = chemical_step1[use_direct_only_nl48_ag_aa +use_direct_only_nl48_nonag_aa].sum(axis=1)

# Calculates factors that will be used for adjustment
# Ag factor : sum of ag uses/ ag composite; Non-Ag factor sum of non ag use/ non ag composite;
# Composite factor sum of Ag and Non Ag composite / AA
# Ag factor is applied to all Ag layer, Non-Ag and Composite factor is applied to JUST the Non-Ag uses
# Parameterr set by ESA team Summer/Fall 2017
# Ag factor was added to Step 2 in Spring of 2018

chemical_step1 ['CONUS_Ag_Ag_Factor'] = chemical_step1['CONUS_Sum_Ag'].div((chemical_step1[use_direct_only_conus_ag_aa[0]]).where(chemical_step1[use_direct_only_conus_ag_aa[0]]!= 0, np.nan), axis = 0)
chemical_step1 ['NL48_Ag_Ag_Factor'] = chemical_step1['NL48_Sum_Ag'].div((chemical_step1[use_direct_only_nl48_ag_aa[0]]).where(chemical_step1[use_direct_only_nl48_ag_aa[0]]!= 0, np.nan), axis = 0)

if not skip_non_ag_adjustment:
    chemical_step1 ['CONUS_NonAg_NonAg_Factor'] = chemical_step1['CONUS_Sum_NonAg'].div((chemical_step1[use_direct_only_conus_nonag_aa[0]]).where(chemical_step1[use_direct_only_conus_nonag_aa[0]]!= 0, np.nan), axis = 0)
    chemical_step1 ['NL48_NonAg_NonAg_Factor'] = chemical_step1['NL48_Sum_NonAg'].div((chemical_step1[use_direct_only_nl48_nonag_aa[0]]).where(chemical_step1[use_direct_only_nl48_nonag_aa[0]]!= 0, np.nan), axis = 0)
    chemical_step1 ['CONUS_Composite_Factor'] = chemical_step1['CONUS_Sum_Composites'].div((chemical_step1[use_direct_only_conus_aa[0]]).where(chemical_step1[use_direct_only_conus_aa[0]]!= 0, np.nan), axis = 0)
    chemical_step1 ['NL48_Composite_Factor'] = chemical_step1['NL48_Sum_Composites'].div((chemical_step1[use_direct_only_nl48_aa[0]]).where(chemical_step1[use_direct_only_nl48_aa[0]]!= 0, np.nan), axis = 0)

if not skip_non_ag_adjustment:
    chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','CONUS_NonAg_NonAg_Factor','NL48_Ag_Ag_Factor','NL48_NonAg_NonAg_Factor','CONUS_Composite_Factor','NL48_Composite_Factor']]= chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','CONUS_NonAg_NonAg_Factor','NL48_Ag_Ag_Factor','NL48_NonAg_NonAg_Factor','CONUS_Composite_Factor','NL48_Composite_Factor']].fillna(0)
else:
    chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','NL48_Ag_Ag_Factor']]= chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','NL48_Ag_Ag_Factor']].fillna(0)

# Applies the factor adjustments Ag use layer by Ag factor, Non Ag use layers by both NonAg factor and composite factor
chemical_step1.ix[:,use_direct_only_conus_ag] = chemical_step1.ix[:,use_direct_only_conus_ag].div((chemical_step1['CONUS_Ag_Ag_Factor']).where(chemical_step1['CONUS_Ag_Ag_Factor']!= 0, np.nan), axis = 0)
chemical_step1.ix[:,use_direct_only_nl48_ag] = chemical_step1.ix[:,use_direct_only_nl48_ag].div(chemical_step1['NL48_Ag_Ag_Factor'].where(chemical_step1['NL48_Ag_Ag_Factor']!= 0, np.nan), axis = 0)
if not skip_non_ag_adjustment:
    chemical_step1.ix[:,use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].div(chemical_step1['CONUS_NonAg_NonAg_Factor'].where(chemical_step1['CONUS_NonAg_NonAg_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].div(chemical_step1['NL48_NonAg_NonAg_Factor'].where(chemical_step1['NL48_NonAg_NonAg_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].div(chemical_step1['CONUS_Composite_Factor'].where(chemical_step1['CONUS_Composite_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].div(chemical_step1['NL48_Composite_Factor'].where(chemical_step1['NL48_Composite_Factor']!= 0, np.nan), axis = 0)

chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')
chemical_step1.fillna(0, inplace=True)
chemical_step1['EntityID'] = chemical_step1['EntityID'].map(lambda x: x).astype(str)

aa_l48 = chemical_step1 [conus_cols]
aa_nl48 = chemical_step1[nl48_cols_f]
aa_l48.to_csv(out_path_redundancy + os.sep + 'CONUS_Step2_Intervals_' + file_type + chemical_name + '.csv')
aa_nl48.to_csv(out_path_redundancy + os.sep + 'NL48_Step2_Intervals_' + file_type + chemical_name + '.csv')


# STEP 3 - ON/ OFF ADJUSTMENTS

# on off field adjustments- direct overlap is set to zero if for species identified as not be present on the on/off
# habitat group types  - decided to set to zero rather than re-calucating the overlap dut to the impact of drift in
# for the whole range; if pixels are remove a new acre value for the species would need to be calculated and drift
# overlap would be greater than 100 unless the drift pixel are also removed - it is possible to do this but not in the
# time frame provided so it was decided to set the direct overlap to zero Fall 2017

# ##Filter L48 AA
out_path_on_off = out_path + os.sep + 'On_Off_Field'
create_directory(out_path_on_off)

cult_use_cols = []
for z in conus_cols:
    for p in on_off_cult_cols:
        if z.startswith(p):
            cult_use_cols.append(z)

pasture_use_cols = []
for z in conus_cols:
    for p in on_off_pasture_cols:
        if z.startswith(p):
            pasture_use_cols.append(z)

# orchard_use_cols = []
# for z in conus_cols:
#     for p in on_off_orchard_cols:
#         if z.startswith(p):
#           if z.endswith("_0"):
#               orchard_use_cols.append(z)


aa_l48.apply(lambda row: on_off_field(row, cult_use_cols, aa_l48, on_off_cult_species), axis=1)
aa_l48.apply(lambda row: on_off_field(row, pasture_use_cols, aa_l48, on_off_pasture_species), axis=1)
aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
aa_l48.fillna(0, inplace=True)
aa_l48.to_csv(out_path_on_off + os.sep + file_type + 'CONUS_Step2_Intervals_' + chemical_name + '.csv')

# # ##Filter NL48 AA

cult_use_cols = []
for z in nl48_cols_f:
    for p in on_off_cult_cols:
        if z.startswith(p):
            cult_use_cols.append(z)

pasture_use_cols = []
for z in nl48_cols_f:
    for p in on_off_pasture_cols:
        if z.startswith(p):
            pasture_use_cols.append(z)

# orchard_use_cols = []
# for z in nl48_cols_f:
#     for p in on_off_orchard_cols:
#         if z.startswith(p):
#           if z.endswith("_0"):
#               orchard_use_cols.append(z)

aa_nl48.apply(lambda row: on_off_field(row, cult_use_cols, aa_nl48, on_off_cult_species), axis=1)
aa_nl48.apply(lambda row: on_off_field(row, pasture_use_cols, aa_nl48, on_off_pasture_species), axis=1)
aa_nl48 = pd.merge(base_sp_df, aa_nl48, on='EntityID', how='left')
aa_l48.fillna(0, inplace=True)
# removed date sharepoint
aa_nl48.to_csv(out_path_on_off + os.sep + file_type + 'NL48_Step2_Intervals_' + chemical_name + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
