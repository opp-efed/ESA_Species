import pandas as pd
import os
import datetime

# TODO Remove extra drift call for for uses that are just aerial ot ground if needed
chemical_name = 'Diazinon'

use_lookup = r'L:\ESA\Results\diazinon\RangeUses_lookup.csv'
max_drift = '765'
l48_BE_sum = r'L:\ESA\Results\diazinon\Tabulated_usage\Diazinon\max\SprayInterval_IntStep_30_MaxDistance_1501' \
             r'\Upper_AllUses_BE_NL48_20180214.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country','Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


out_location = r'L:\ESA\Results\diaz_example\outtables'


on_off_cult_species = []

# on_off_cult_species = ['4', '5', '9', '17', '21', '28', '29', '31', '32', '33', '34', '35', '37', '38', '39', '40',
#                        '41', '42', '43', '46', '49', '50', '51', '53', '54', '55', '56', '57', '58', '59', '60', '61',
#                        '62', '63', '65', '74', '79', '80', '81', '84', '85', '86', '87', '94', '95', '96', '97', '98',
#                        '99', '102', '103', '107', '112', '113', '115', '116', '120', '121', '126', '127', '128', '129',
#                        '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '142', '145', '149',
#                        '150', '151', '152', '156', '162', '163', '164', '166', '167', '168', '169', '170', '171', '172',
#                        '174', '175', '176', '177', '178', '179', '180', '182', '183', '185', '192', '196', '198', '200',
#                        '387', '389', '390', '391', '392', '393', '394', '395', '419', '420', '421', '423', '424', '425',
#                        '426', '427', '428', '429', '430', '431', '432', '433', '434', '437', '438', '443', '444', '446',
#                        '450', '451', '452', '455', '456', '457', '458', '462', '496', '497', '498', '500', '501', '502',
#                        '503', '504', '505', '506', '507', '508', '509', '510', '511', '512', '513', '514', '515', '516',
#                        '518', '519', '520', '521', '523', '524', '525', '526', '528', '529', '530', '531', '532', '533',
#                        '535', '536', '537', '539', '540', '541', '542', '543', '544', '545', '546', '547', '548', '549',
#                        '550', '551', '552', '553', '554', '555', '556', '557', '558', '559', '560', '561', '562', '563',
#                        '564', '565', '566', '567', '569', '570', '571', '572', '573', '574', '575', '576', '577', '578',
#                        '579', '581', '584', '585', '586', '587', '588', '589', '590', '591', '592', '593', '594', '595',
#                        '596', '597', '598', '599', '600', '601', '602', '603', '604', '605', '607', '608', '609', '610',
#                        '611', '612', '613', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '624',
#                        '626', '627', '628', '629', '630', '631', '632', '633', '634', '635', '636', '637', '638', '639',
#                        '640', '641', '642', '643', '644', '645', '646', '647', '648', '649', '650', '651', '652', '653',
#                        '654', '655', '656', '657', '658', '659', '660', '661', '662', '663', '664', '665', '666', '667',
#                        '668', '669', '670', '671', '672', '673', '674', '675', '676', '679', '680', '681', '682', '683',
#                        '684', '685', '686', '687', '688', '689', '690', '691', '692', '693', '694', '695', '696', '698',
#                        '700', '701', '702', '703', '704', '705', '707', '708', '709', '710', '712', '713', '715', '716',
#                        '717', '718', '719', '720', '721', '722', '723', '724', '725', '726', '727', '728', '729', '730',
#                        '731', '732', '733', '734', '735', '736', '737', '738', '739', '740', '741', '742', '743', '744',
#                        '745', '746', '747', '748', '749', '750', '751', '752', '753', '754', '755', '756', '757', '758',
#                        '759', '760', '761', '762', '763', '764', '765', '766', '767', '768', '769', '770', '771', '772',
#                        '773', '774', '775', '776', '777', '778', '779', '780', '781', '782', '783', '784', '788', '789',
#                        '790', '791', '792', '793', '794', '795', '796', '797', '798', '800', '801', '802', '803', '804',
#                        '805', '806', '808', '809', '810', '811', '812', '813', '814', '815', '816', '817', '818', '819',
#                        '820', '821', '822', '824', '825', '826', '827', '828', '829', '830', '831', '832', '833', '835',
#                        '836', '837', '838', '839', '840', '841', '842', '843', '844', '845', '846', '847', '848', '849',
#                        '850', '851', '852', '853', '854', '855', '856', '857', '859', '860', '861', '862', '863', '864',
#                        '865', '866', '867', '868', '869', '871', '872', '873', '874', '876', '878', '879', '880', '881',
#                        '882', '883', '884', '885', '886', '887', '888', '889', '890', '891', '892', '893', '894', '895',
#                        '896', '897', '898', '899', '900', '901', '902', '903', '904', '905', '906', '907', '908', '909',
#                        '910', '911', '912', '913', '914', '915', '916', '917', '919', '920', '921', '922', '923', '924',
#                        '925', '926', '927', '928', '929', '930', '932', '933', '934', '935', '936', '937', '938', '939',
#                        '940', '941', '942', '943', '945', '946', '947', '948', '949', '950', '951', '952', '953', '954',
#                        '955', '956', '957', '958', '959', '960', '961', '962', '963', '964', '965', '966', '967', '968',
#                        '969', '970', '971', '972', '973', '974', '975', '976', '977', '978', '979', '980', '981', '982',
#                        '983', '984', '985', '986', '987', '988', '989', '990', '991', '992', '993', '994', '995', '996',
#                        '997', '998', '999', '1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008',
#                        '1009', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', '1018', '1019', '1020',
#                        '1021', '1022', '1023', '1024', '1025', '1026', '1027', '1029', '1030', '1031', '1032', '1033',
#                        '1034', '1035', '1036', '1037', '1038', '1039', '1040', '1041', '1042', '1043', '1044', '1045',
#                        '1046', '1048', '1049', '1050', '1051', '1052', '1053', '1054', '1055', '1056', '1057', '1058',
#                        '1059', '1061', '1062', '1063', '1065', '1066', '1067', '1068', '1069', '1070', '1071', '1072',
#                        '1073', '1074', '1075', '1076', '1077', '1078', '1079', '1080', '1081', '1082', '1083', '1084',
#                        '1085', '1086', '1087', '1088', '1089', '1090', '1091', '1092', '1093', '1094', '1095', '1096',
#                        '1097', '1098', '1099', '1101', '1103', '1104', '1105', '1106', '1107', '1108', '1109', '1110',
#                        '1111', '1112', '1113', '1114', '1115', '1116', '1117', '1118', '1119', '1120', '1121', '1122',
#                        '1123', '1124', '1125', '1126', '1127', '1128', '1129', '1130', '1131', '1132', '1133', '1134',
#                        '1135', '1136', '1137', '1138', '1139', '1140', '1141', '1143', '1144', '1145', '1146', '1147',
#                        '1148', '1149', '1150', '1151', '1152', '1153', '1154', '1155', '1156', '1157', '1158', '1159',
#                        '1160', '1162', '1163', '1164', '1165', '1166', '1167', '1168', '1169', '1170', '1171', '1172',
#                        '1173', '1174', '1175', '1176', '1177', '1178', '1179', '1180', '1181', '1182', '1183', '1184',
#                        '1185', '1186', '1188', '1189', '1190', '1191', '1192', '1193', '1194', '1195', '1196', '1197',
#                        '1198', '1200', '1201', '1202', '1205', '1206', '1207', '1208', '1209', '1210', '1211', '1212',
#                        '1213', '1214', '1215', '1216', '1217', '1218', '1219', '1220', '1221', '1222', '1223', '1224',
#                        '1225', '1226', '1227', '1229', '1230', '1231', '1232', '1233', '1234', '1235', '1241', '1250',
#                        '1252', '1253', '1254', '1255', '1256', '1258', '1259', '1262', '1263', '1264', '1265', '1266',
#                        '1267', '1278', '1283', '1311', '1349', '1378', '1400', '1407', '1415', '1497', '1502', '1521',
#                        '1525', '1535', '1607', '1609', '1623', '1632', '1636', '1645', '1678', '1693', '1709', '1710',
#                        '1760', '1783', '1831', '1840', '1862', '1881', '1935', '1968', '1984', '1989', '2036', '2085',
#                        '2118', '2154', '2211', '2265', '2268', '2273', '2278', '2364', '2404', '2458', '2517', '2567',
#                        '2619', '2682', '2683', '2727', '2730', '2758', '2778', '2780', '2782', '2810', '2823', '2860',
#                        '2934', '2970', '3020', '3049', '3054', '3084', '3116', '3154', '3175', '3194', '3224', '3267',
#                        '3271', '3292', '3385', '3387', '3388', '3412', '3472', '3540', '3592', '3653', '3670', '3671',
#                        '3722', '3728', '3737', '3753', '3784', '3832', '3849', '3871', '3876', '3990', '3999', '4000',
#                        '4007', '4030', '4136', '4179', '4201', '4228', '4237', '4238', '4253', '4297', '4308', '4377',
#                        '4395', '4413', '4420', '4487', '4508', '4533', '4551', '4564', '4565', '4589', '4630', '4680',
#                        '4712', '4724', '4740', '4754', '4858', '4889', '4910', '5067', '5168', '5170', '5186', '5210',
#                        '5233', '5273', '5334', '5468', '5580', '5610', '5709', '5763', '5956', '6019', '6097', '6176',
#                        '6257', '6303', '6490', '6522', '6536', '6617', '6620', '6632', '6672', '6679', '6782', '6845',
#                        '6870', '6901', '6969', '7046', '7054', '7067', '7116', '7136', '7167', '7170', '7206', '7220',
#                        '7229', '7254', '7261', '7280', '7367', '7495', '7529', '7572', '7617', '7731', '7805', '7840',
#                        '7886', '7892', '7907', '7918', '7948', '7979', '7992', '8083', '8166', '8254', '8277', '8303',
#                        '8336', '8338', '8347', '8357', '8392', '8503', '8621', '8683', '8684', '8685', '8962', '9001',
#                        '9338', '9721', '9725', '9929', '9951', '9952', '9953', '9954', '9955', '9956', '9957', '9958',
#                        '9959', '9960', '9961', '9962', '9963', '9965', '10007', '10008', '10009', '10021', '10028',
#                        '10034', '10043', '10076', '10147', '10161', '10178', '10222', '10223', '10224', '10225',
#                        '10227', '10228', '10229', '10231', '10232', '10233', '10234', '10235', '10290', '10479',
#                        '10480', '10481', '10483', '10583', '10584', '10585', '10586', '10587', '10588', '10590',
#                        '10591', '10593', '10594', '10599', '10719', '10720', '10721', '10722', '10723', '10724',
#                        '10725', '10726', '10727', '10728', '10729', '10732', '10909', '11260', '11340']

on_off_orchards_species = []

find_file_type = os.path.basename(l48_BE_sum)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def step_1_ED(row, col_l48):

    if row[col_l48] < 0.44:
        return 'No Effect - Overlap'
    elif row[col_l48] < 4.5:
        return 'NLAA - Overlap - 5percent'
    elif row['CONUS_Federal Lands_0'] >= 98.5 :
        return 'No Effect- Federal Lands'
    elif row['CONUS_Federal Lands_0'] >= 94.5:
        return 'NLAA - Federal Land'
    elif row[col_l48] >= 4.5:
        return 'MA'
    else:
        return 'Check result - error'


def NLAA_overlap(row):
    max_col = ['CONUS_Vegetables and Ground Fruit_305','CONUS_Orchards and Vineyards_305']
    list_values = row[max_col]
    if max(list_values) < 0.45:
        return 'NLAA Overlap - 1percent'
    else:
        return row['Step 2 ED Comment']

def on_off_field(row, cols, df, on_off_species):
    ent_id = row['EntityID']
    if ent_id in on_off_species:
        col = [v for v in cols if v.endswith("_0")]
        for i in col:
            df.loc[df['EntityID'] == ent_id, i] = 0
        for other_col in cols:
            if other_col.endswith("_0"):
                pass
            else:
                direct_over = row[other_col.split("_")[0] + "_" + other_col.split("_")[1] + "_0"]
                value = row[other_col]
                df.loc[df['EntityID'] == ent_id, other_col] = value - direct_over
    else:
        pass


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_sum)

list_final_uses = list(set(use_lookup_df['FinalUseHeader'].values.tolist()))

on_off_cult = use_lookup_df.loc[(use_lookup_df['On/Off_AG'] == 'x')]

# on_off_orchard = use_lookup_df.loc[(use_lookup_df['On/Off_Orchard_Plantation'] == 'x')]

on_off_cult_cols = on_off_cult['FinalColHeader'].values.tolist()

# on_off_orchard_cols = on_off_orchard['FinalColHeader'].values.tolist()

collapsed_dict = {}

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)
# ##Filter L48 AA
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Included AA'] == 'x'))]

col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

col_selection_aa = ['EntityID']
for col in col_prefix_CONUS:
    col_selection_aa.append(col + "_0")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['ground'] == 'x')]) > 0:
        col_selection_aa.append(col + "_305")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
        col_selection_aa.append(col + "_765")

cols_w_overlap_l48 = [v for v in l48_df.columns.values.tolist() if v in col_selection_aa]


l48_df = l48_df[cols_w_overlap_l48]
l48_df = l48_df.reindex(columns=col_selection_aa)

chemical_step1 = l48_df[col_selection_aa]
print col_selection_aa
final_use = aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')], [
    'FinalColHeader']
if len(aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
    final_use = str(final_use) + '_765'
else:
    final_use = str(final_use) + '_305'

chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')



binned_use = []
for x in chemical_step1.columns.values.tolist():
    if x in col_include_output:
        pass
    else:
        if x.split("_")[0] + "_" + x.split("_")[1] not in binned_use and x.split("_")[1] != 'Federal Lands':
            binned_use.append(x.split("_")[0] + "_" + x.split("_")[1])

cult_use_cols = []
for z in chemical_step1.columns.values.tolist():
    for p in on_off_cult_cols:
        if z.startswith(p):
            cult_use_cols.append(z)


chemical_step1.apply(lambda row: on_off_field(row, cult_use_cols, chemical_step1, on_off_cult_species), axis=1)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: step_1_ED(row, 'CONUS_' + chemical_name + " AA""_" + max_drift),axis=1)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: NLAA_overlap(row),axis=1)
chemical_step1.to_csv(out_path + os.sep + 'GIS_Step2_' + file_type + chemical_name + '.csv')

conus_cols = [v for v in chemical_step1.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]
conus_df_step1 = chemical_step1[conus_cols]
conus_df_step1.to_csv(out_path + os.sep + 'CONUS_Step2_' + file_type + chemical_name + '.csv')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
