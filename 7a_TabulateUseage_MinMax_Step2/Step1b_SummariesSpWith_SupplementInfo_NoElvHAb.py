import pandas as pd

import datetime
import os

in_directory_csv = r'L:\Workspace\StreamLine\ESA\Results_HUCAB\NL48\Range\Agg_Layers'
out_path = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB'
out_poltical = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB\PolBoundaries'


look_up_fc_ab = r'D:\Lookup_R_Clipped_Union_CntyInter_HUC2ABInter_20180612'
# look_up_fc = r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\Range\R_Clipped_Union_20180110.gdb'

grid_folder_lookup = {'AK': 'AK_WGS_1984_Albers',
                      'AS': 'AS_WGS_1984_UTM_Zone_2S',
                      'CNMI': 'CNMI_WGS_1984_UTM_Zone_55N',
                      'CONUS': 'CONUS_Albers_Conical_Equal_Area',
                      'GU': 'GU_WGS_1984_UTM_Zone_55N',
                      'HI': 'HI_NAD_1983_UTM_Zone_4N',
                      'PR': 'PR_Albers_Conical_Equal_Area',
                      'VI': 'VI_WGS_1984_UTM_Zone_20N'}



run_habitat = False
run_elevation = False
run_elevation_hab = False
run_aqu = True

skip_species = []

types_dict = {'VALUE_1380': int, 'VALUE_1381': int, 'VALUE_1382': int,
              'VALUE_1384': int, 'VALUE_1385': int, 'VALUE_1386': int,
              'VALUE_1387': int, 'VALUE_1223': int, 'VALUE_1389': int,
              'VALUE_1221': int, 'VALUE_1225': int, 'VALUE_1224': int,
              'VALUE_836': int, 'VALUE_834': int, 'VALUE_835': int,
              'VALUE_496': int, 'VALUE_831': int, 'VALUE_254': int,
              'VALUE_1094': int, 'VALUE_1090': int, 'VALUE_1214': int,
              'VALUE_1092': int, 'VALUE_1318': int, 'VALUE_1317': int,
              'VALUE_1315': int, 'VALUE_1215': int, 'VALUE_1098': int,
              'VALUE_1312': int, 'VALUE_1310': int, 'VALUE_1218': int,
              'VALUE_1388': int, 'VALUE_1209': int, 'VALUE_1168': int,
              'VALUE_785': int, 'VALUE_787': int, 'VALUE_469': int,
              'VALUE_468': int, 'VALUE_782': int, 'VALUE_465': int,
              'VALUE_466': int, 'VALUE_1164': int, 'VALUE_789': int,
              'VALUE_1166': int, 'VALUE_1167': int, 'VALUE_543': int,
              'VALUE_540': int, 'VALUE_1369': int, 'VALUE_1006': int,
              'VALUE_1005': int, 'VALUE_1363': int, 'VALUE_1360': int,
              'VALUE_1008': int, 'VALUE_1366': int, 'VALUE_1364': int,
              'VALUE_547': int, 'VALUE_768': int, 'VALUE_606': int,
              'VALUE_600': int, 'VALUE_948': int, 'VALUE_602': int,
              'VALUE_947': int, 'VALUE_766': int, 'VALUE_764': int,
              'VALUE_941': int, 'VALUE_1111': int, 'VALUE_1113': int,
              'VALUE_833': int, 'VALUE_308': int, 'VALUE_300': int,
              'VALUE_301': int, 'VALUE_268': int, 'VALUE_305': int,
              'VALUE_1275': int, 'VALUE_1277': int, 'VALUE_1266': int,
              'VALUE_1265': int, 'VALUE_1264': int, 'VALUE_1263': int,
              'VALUE_1261': int, 'VALUE_1260': int, 'VALUE_283': int,
              'VALUE_284': int, 'VALUE_1500': int, 'VALUE_1268': int,
              'VALUE_1461': int, 'VALUE_1463': int, 'VALUE_1462': int,
              'VALUE_1465': int, 'VALUE_1464': int, 'VALUE_1467': int,
              'VALUE_1466': int, 'VALUE_169': int, 'VALUE_1142': int,
              'VALUE_161': int, 'VALUE_1143': int, 'VALUE_1141': int,
              'VALUE_67': int, 'VALUE_60': int, 'VALUE_1213': int,
              'VALUE_1210': int, 'VALUE_849': int, 'VALUE_848': int,
              'VALUE_846': int, 'VALUE_845': int, 'VALUE_844': int,
              'VALUE_842': int, 'VALUE_840': int, 'VALUE_1448': int,
              'VALUE_1148': int, 'VALUE_1026': int, 'VALUE_1442': int,
              'VALUE_1379': int, 'VALUE_1049': int, 'VALUE_509': int,
              'VALUE_1047': int, 'VALUE_1046': int, 'VALUE_1044': int,
              'VALUE_502': int, 'VALUE_1041': int, 'VALUE_1040': int,
              'VALUE_1326': int, 'VALUE_1288': int, 'VALUE_1324': int,
              'VALUE_1325': int, 'VALUE_1323': int, 'VALUE_1320': int,
              'VALUE_1321': int, 'VALUE_1281': int, 'VALUE_1072': int,
              'VALUE_1282': int, 'VALUE_1284': int, 'VALUE_1328': int,
              'VALUE_726': int, 'VALUE_330': int, 'VALUE_724': int,
              'VALUE_664': int, 'VALUE_722': int, 'VALUE_720': int,
              'VALUE_721': int, 'VALUE_339': int, 'VALUE_484': int,
              'VALUE_483': int, 'VALUE_729': int, 'VALUE_1373': int,
              'VALUE_1398': int, 'VALUE_432': int, 'VALUE_1132': int,
              'VALUE_1039': int, 'VALUE_436': int, 'VALUE_342': int,
              'VALUE_1134': int, 'VALUE_1032': int, 'VALUE_1033': int,
              'VALUE_1030': int, 'VALUE_1138': int, 'VALUE_1036': int,
              'VALUE_349': int, 'VALUE_1034': int, 'VALUE_1035': int,
              'VALUE_973': int, 'VALUE_658': int, 'VALUE_150': int,
              'VALUE_152': int, 'VALUE_1043': int, 'VALUE_657': int,
              'VALUE_655': int, 'VALUE_937': int, 'VALUE_934': int,
              'VALUE_933': int, 'VALUE_931': int, 'VALUE_930': int,
              'VALUE_1425': int, 'VALUE_1426': int, 'VALUE_1421': int,
              'VALUE_939': int, 'VALUE_1422': int, 'Unnamed: 0': int,
              'VALUE_975': int, 'VALUE_611': int, 'VALUE_234': int,
              'VALUE_755': int, 'VALUE_1397': int, 'VALUE_1395': int,
              'VALUE_1394': int, 'VALUE_751': int, 'VALUE_1391': int,
              'VALUE_1259': int, 'VALUE_1256': int, 'VALUE_800': int,
              'VALUE_807': int, 'VALUE_806': int, 'VALUE_1250': int,
              'VALUE_804': int, 'VALUE_1499': int, 'VALUE_1490': int,
              'VALUE_1491': int, 'VALUE_1492': int, 'VALUE_1493': int,
              'VALUE_1494': int, 'VALUE_1497': int, 'VALUE_331': int,
              'VALUE_1314': int, 'VALUE_725': int, 'VALUE_1083': int,
              'VALUE_335': int, 'VALUE_1081': int, 'VALUE_1080': int,
              'VALUE_1087': int, 'VALUE_1086': int, 'VALUE_1084': int,
              'VALUE_30': int, 'VALUE_488': int, 'VALUE_1368': int,
              'VALUE_1341': int, 'VALUE_480': int, 'VALUE_711': int,
              'VALUE_1179': int, 'VALUE_1178': int, 'VALUE_1176': int,
              'VALUE_474': int, 'VALUE_1173': int, 'VALUE_1171': int,
              'VALUE_1170': int, 'VALUE_577': int, 'VALUE_576': int,
              'VALUE_1074': int, 'VALUE_1075': int, 'VALUE_573': int,
              'VALUE_1073': int, 'VALUE_1070': int, 'VALUE_570': int,
              'VALUE_1371': int, 'VALUE_1370': int, 'VALUE_84': int,
              'VALUE_1374': int, 'VALUE_579': int, 'VALUE_1376': int,
              'VALUE_201': int, 'VALUE_617': int, 'VALUE_971': int,
              'VALUE_615': int, 'VALUE_977': int, 'VALUE_976': int,
              'VALUE_759': int, 'VALUE_758': int, 'VALUE_757': int,
              'VALUE_979': int, 'VALUE_752': int, 'VALUE_618': int,
              'VALUE_750': int, 'VALUE_1116': int, 'VALUE_685': int,
              'VALUE_684': int, 'VALUE_680': int, 'VALUE_276': int,
              'VALUE_270': int, 'VALUE_271': int, 'VALUE_375': int,
              'VALUE_379': int, 'VALUE_1100': int, 'VALUE_1101': int,
              'VALUE_371': int, 'VALUE_1234': int, 'VALUE_108': int,
              'OBJECTID': int, 'VALUE_1236': int, 'VALUE_1103': int,
              'VALUE_1077': int, 'VALUE_296': int, 'VALUE_295': int,
              'VALUE_1455': int, 'VALUE_1456': int, 'VALUE_1451': int,
              'VALUE_1452': int, 'VALUE_1453': int, 'VALUE_1106': int,
              'VALUE_1458': int, 'VALUE_1459': int, 'VALUE_192': int,
              'VALUE_1248': int, 'VALUE_829': int, 'VALUE_1449': int,
              'VALUE_1247': int, 'VALUE_999': int, 'VALUE_998': int,
              'VALUE_852': int, 'VALUE_853': int, 'VALUE_1205': int,
              'VALUE_1204': int, 'VALUE_1207': int, 'VALUE_1206': int,
              'VALUE_858': int, 'VALUE_859': int, 'VALUE_993': int,
              'VALUE_994': int, 'VALUE_997': int, 'VALUE_1130': int,
              'VALUE_1243': int, 'VALUE_1135': int, 'VALUE_1149': int,
              'VALUE_780': int, 'VALUE_531': int, 'VALUE_530': int,
              'VALUE_536': int, 'VALUE_534': int, 'VALUE_1337': int,
              'VALUE_1336': int, 'VALUE_1331': int, 'VALUE_1332': int,
              'VALUE_1339': int, 'VALUE_323': int, 'VALUE_492': int,
              'VALUE_494': int, 'VALUE_324': int, 'VALUE_499': int,
              'VALUE_1482': int, 'VALUE_1002': int, 'VALUE_1001': int,
              'VALUE_1146': int, 'VALUE_1145': int, 'VALUE_403': int,
              'VALUE_402': int, 'VALUE_1140': int, 'VALUE_400': int,
              'VALUE_1021': int, 'VALUE_1020': int, 'VALUE_1023': int,
              'VALUE_1025': int, 'VALUE_1027': int, 'VALUE_408': int,
              'VALUE_882': int, 'VALUE_1406': int, 'VALUE_886': int,
              'VALUE_885': int, 'VALUE_920': int, 'VALUE_666': int,
              'VALUE_888': int, 'VALUE_924': int, 'VALUE_662': int,
              'VALUE_926': int, 'VALUE_660': int, 'VALUE_1410': int,
              'VALUE_1411': int, 'VALUE_1412': int, 'VALUE_1413': int,
              'VALUE_1415': int, 'VALUE_1416': int, 'VALUE_1417': int,
              'VALUE_1418': int, 'VALUE_1195': int, 'VALUE_394': int,
              'VALUE_391': int, 'VALUE_390': int, 'VALUE_1361': int,
              'VALUE_1198': int, 'VALUE_713': int, 'VALUE_1429': int,
              'VALUE_247': int, 'VALUE_241': int, 'VALUE_240': int,
              'VALUE_715': int, 'VALUE_1053': int, 'VALUE_607': int,
              'VALUE_1055': int, 'VALUE_818': int, 'VALUE_819': int,
              'VALUE_814': int, 'VALUE_1244': int, 'VALUE_816': int,
              'VALUE_810': int, 'VALUE_1240': int, 'VALUE_812': int,
              'VALUE_1242': int, 'VALUE_603': int, 'VALUE_1489': int,
              'VALUE_1487': int, 'VALUE_1485': int, 'VALUE_1484': int,
              'VALUE_1351': int, 'VALUE_1481': int, 'VALUE_1480': int,
              'VALUE_1350': int, 'VALUE_1423': int, 'VALUE_763': int,
              'VALUE_1308': int, 'VALUE_942': int, 'VALUE_0': int,
              'VALUE_1359': int, 'VALUE_1358': int, 'VALUE_863': int,
              'VALUE_865': int, 'VALUE_1301': int, 'VALUE_595': int,
              'VALUE_597': int, 'VALUE_591': int, 'VALUE_590': int,
              'VALUE_593': int, 'VALUE_1182': int, 'VALUE_569': int,
              'VALUE_1180': int, 'VALUE_1181': int, 'VALUE_1186': int,
              'VALUE_1187': int, 'VALUE_445': int, 'VALUE_560': int,
              'VALUE_1188': int, 'VALUE_563': int, 'VALUE_566': int,
              'VALUE_1065': int, 'VALUE_90': int, 'VALUE_1061': int,
              'VALUE_94': int, 'VALUE_1063': int, 'VALUE_1344': int,
              'VALUE_1347': int, 'VALUE_1340': int, 'VALUE_1068': int,
              'VALUE_1343': int, 'VALUE_964': int, 'VALUE_742': int,
              'VALUE_967': int, 'VALUE_960': int, 'VALUE_745': int,
              'VALUE_218': int, 'VALUE_968': int, 'VALUE_969': int,
              'VALUE_699': int, 'VALUE_697': int, 'VALUE_695': int,
              'VALUE_692': int, 'VALUE_690': int, 'VALUE_366': int,
              'VALUE_1110': int, 'VALUE_364': int, 'VALUE_362': int,
              'VALUE_1114': int, 'VALUE_360': int, 'VALUE_361': int,
              'VALUE_134': int, 'VALUE_910': int, 'VALUE_913': int,
              'VALUE_912': int, 'VALUE_915': int, 'VALUE_914': int,
              'VALUE_917': int, 'VALUE_1443': int, 'VALUE_918': int,
              'VALUE_1441': int, 'VALUE_1440': int, 'VALUE_1447': int,
              'VALUE_1444': int, 'VALUE_182': int, 'VALUE_180': int,
              'VALUE_189': int, 'VALUE_626': int, 'VALUE_624': int,
              'VALUE_1201': int, 'VALUE_1200': int, 'VALUE_1203': int,
              'VALUE_42': int, 'VALUE_825': int, 'VALUE_1235': int,
              'VALUE_827': int, 'VALUE_989': int, 'VALUE_1230': int,
              'VALUE_1231': int, 'VALUE_823': int, 'VALUE_1233': int,
              'VALUE_982': int, 'VALUE_980': int, 'VALUE_986': int,
              'VALUE_1239': int, 'VALUE_984': int, 'VALUE_991': int,
              'VALUE_990': int, 'VALUE_1392': int, 'VALUE_524': int,
              'VALUE_1309': int, 'VALUE_1056': int, 'VALUE_523': int,
              'VALUE_1252': int, 'VALUE_1302': int, 'VALUE_1303': int,
              'VALUE_1306': int, 'VALUE_1253': int, 'VALUE_1399': int,
              'VALUE_1251': int, 'VALUE_1129': int, 'VALUE_313': int,
              'VALUE_318': int, 'VALUE_1124': int, 'VALUE_792': int,
              'VALUE_797': int, 'VALUE_794': int, 'VALUE_798': int,
              'VALUE_416': int, 'VALUE_417': int, 'VALUE_551': int,
              'VALUE_550': int, 'VALUE_553': int, 'VALUE_1018': int,
              'VALUE_1019': int, 'VALUE_1152': int, 'VALUE_558': int,
              'VALUE_1159': int, 'VALUE_1158': int, 'VALUE_1012': int,
              'VALUE_670': int, 'VALUE_778': int, 'VALUE_674': int,
              'VALUE_891': int, 'VALUE_676': int, 'VALUE_771': int,
              'VALUE_954': int, 'VALUE_957': int, 'VALUE_956': int,
              'VALUE_898': int, 'VALUE_774': int, 'VALUE_953': int,
              'VALUE_952': int, 'VALUE_381': int, 'VALUE_1405': int,
              'VALUE_1404': int, 'VALUE_384': int, 'VALUE_1402': int,
              'VALUE_1400': int, 'VALUE_1408': int, 'VALUE_1154': int,
              'VALUE_1157': int, 'VALUE_708': int, 'VALUE_258': int,
              'VALUE_256': int, 'VALUE_706': int, 'VALUE_700': int,
              'VALUE_702': int, 'VALUE_442': int, 'VALUE_1271': int,
              'VALUE_1272': int, 'VALUE_1273': int, 'VALUE_1274': int,
              'VALUE': int, 'VALUE_1276': int, 'VALUE_1015': int,
              'VALUE_1279': int, 'VALUE_1472': int, 'VALUE_1470': int,
              'VALUE_1471': int, 'VALUE_1476': int, 'VALUE_1477': int,
              'VALUE_1474': int, 'VALUE_1475': int, 'VALUE_174': int,
              'VALUE_894': int, 'VALUE_1189': int, 'VALUE_890': int,
              'VALUE_958': int, 'VALUE_878': int, 'VALUE_1064': int,
              'VALUE_872': int, 'VALUE_873': int, 'VALUE_870': int,
              'VALUE_876': int, 'VALUE_874': int, 'VALUE_740': int,
              'VALUE_678': int, 'VALUE_1348': int, 'VALUE_1060': int,
              'VALUE_582': int, 'VALUE_589': int, 'VALUE_1190': int,
              'VALUE_456': int, 'VALUE_457': int, 'VALUE_450': int,
              'VALUE_1194': int, 'VALUE_1197': int, 'VALUE_453': int,
              'VALUE_1050': int, 'VALUE_1051': int, 'VALUE_517': int,
              'VALUE_516': int, 'VALUE_458': int, 'VALUE_510': int,
              'VALUE_513': int, 'VALUE_512': int, 'VALUE_1353': int,
              'VALUE_1352': int, 'VALUE_1298': int, 'VALUE_1299': int,
              'VALUE_1357': int, 'VALUE_1356': int, 'VALUE_1355': int,
              'VALUE_1292': int, 'VALUE_1293': int, 'VALUE_1290': int,
              'VALUE_1291': int, 'VALUE_1297': int, 'VALUE_1294': int,
              'VALUE_1295': int, 'VALUE_735': int, 'VALUE_212': int,
              'VALUE_737': int, 'VALUE_730': int, 'VALUE_732': int,
              'VALUE_210': int, 'VALUE_228': int, 'VALUE_1082': int,
              'VALUE_216': int, 'VALUE_961': int, 'VALUE_421': int,
              'VALUE_420': int, 'VALUE_351': int, 'VALUE_424': int,
              'VALUE_426': int, 'VALUE_429': int, 'VALUE_1126': int,
              'VALUE_1127': int, 'VALUE_1120': int, 'VALUE_1123': int,
              'VALUE_120': int, 'VALUE_648': int, 'VALUE_123': int,
              'VALUE_127': int, 'VALUE_641': int, 'VALUE_642': int,
              'VALUE_644': int, 'VALUE_647': int, 'VALUE_646': int,
              'VALUE_902': int, 'VALUE_900': int, 'VALUE_901': int,
              'VALUE_906': int, 'VALUE_907': int, 'VALUE_904': int,
              'VALUE_1436': int, 'VALUE_1437': int, 'VALUE_1434': int,
              'VALUE_1435': int, 'VALUE_1432': int, 'VALUE_1430': int,
              'VALUE_1431': int, 'VALUE_1183': int, 'VALUE_966': int,
              'VALUE_635': int, 'VALUE_636': int, 'VALUE_637': int,
              'VALUE_630': int, 'VALUE_632': int}

list_fc = os.listdir(look_up_fc_ab)
list_fc_ab = os.listdir(look_up_fc_ab)
list_dir = os.listdir(in_directory_csv)


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

    df_state = out_df[['EntityID', 'STATEFP', 'STUSPS'] + val_col].copy()
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
    look_up.ix[:,'HUCID'] = look_up.ix[:,'HUCID'].map(lambda z: str(z).split('.')[0]).astype(str)

    # for df in pd.read_csv(in_path, chunksize=chunksize, iterator=True, low_memory=True, dtype=types_dict):
    for df in pd.read_csv(in_path, chunksize=chunksize, iterator=True, low_memory=True):
        # df = pd.read_csv(in_path , low_memory=True,dtype = types_dict)
        if len(df) > 0:
            df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
            df.index += j
            pp += 1
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
            merged_df_other.groupby(['EntityID', 'GEOID', 'STUSPS', 'STATEFP'] )[
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

    list_csv = os.listdir(in_directory_csv + os.sep + folder)  # list of csv in folder
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]  # list of att csvs
    # loops on each csv added the HUCIDs and ZoneIDs from parent fc att table to working dfs, then transforms table
    # so it is entityID by elevation or habitat
    for csv in list_csv:
        if csv.split("_")[0] + "_" + csv.split("_")[1] in skip_species:
            continue
        elif os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEleHaB.csv')):
            continue
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
                # print len(out_all_other), len (out_all_huc)

                # Generates very large files run transforms and summarize to species in memory to keep file size low
                # out_all.to_csv(out_folder + os.sep + csv)


                aqu = pd.DataFrame(columns=[])

                no_adjust_no = pd.DataFrame(columns=[])
                no_adjust_cnty = pd.DataFrame(columns=[])
                no_adjust_state = pd.DataFrame(columns=[])
                if len(out_all_huc) > 0 and len(out_all_other) > 0:
                        # min_row = 0
                        # chunksize = 100000
                        # max_row = chunksize
                        # # Tables summed the the whole species range can be used to QC other output but are not used as
                        # # inputs in any other step
                        # while int(len(out_all_huc)) >= int(min_row) and int(len(out_all_huc))>0:
                        #     loop_sp_table = out_all_huc[(out_all_huc.index >= min_row) & ((out_all_huc.index >= min_row) < max_row)]
                        loop_sp_table = out_all_huc.copy()
                        if not os.path.exists(out_path + os.sep + 'Agg_Layers' + os.sep + folder):
                            os.mkdir(out_path + os.sep + 'Agg_Layers' + os.sep + folder)
                        if run_aqu:
                            if not os.path.exists(
                                    out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace('.csv',
                                                                                                              '_HUC2AB.csv')):
                                aqu = export_aquatics(loop_sp_table, aqu)
                                aqu.to_csv(
                                    out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace('.csv',
                                                                                                              '_HUC2AB.csv'))
                                print '  Exported {0}'.format(
                                    out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace('.csv',
                                                                                                              '_HUC2AB.csv'))
                        #     min_row = max_row
                        #     max_row = max_row + chunksize
                        # # min_row = 0
                        # chunksize = 100000
                        # max_row = chunksize
                        # while int(len(out_all_other)) >= int(min_row) and int(len(out_all_other))>0:

                        # loop_sp_table = out_all_other[(out_all_other.index >= min_row) & ((out_all_other.index >= min_row) < max_row)]
                        loop_sp_table = out_all_other.copy()
                        if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')):
                            no_adjust_working, no_adjust_no, no_adjust_cnty, no_adjust_state = no_adjust(loop_sp_table,
                                                                                                         no_adjust_no,
                                                                                                         no_adjust_cnty,
                                                                                                         no_adjust_state)
                            no_adjust_no.to_csv(
                                out_path + os.sep + 'Agg_Layers' + os.sep + folder + os.sep + csv.replace('.csv',
                                                                                                          '_noadjust.csv'))
                            no_adjust_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_noadjust.csv'))
                            no_adjust_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))
                            print '  Exported {0}'.format(
                                out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))


                        # min_row = max_row
                        # max_row = max_row + chunksize

            else:
                print ('Already created all tables, the last table is: {0}'.format((out_pol_folder_st + os.sep + csv)))

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
