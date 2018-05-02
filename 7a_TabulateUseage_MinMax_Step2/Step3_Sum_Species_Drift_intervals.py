import pandas as pd
import os
import datetime
import numpy as np

in_location = r'L:\ESA\Results\diazinon\Tabulated_usage\Diazinon\example'
use_lookup = r'L:\ESA\Results\diazinon\RangeUses_lookup.csv'
in_acres_table = r'L:\ESA\CompositeFiles_Winter2018\R_Acres_by_region_20180110_GAP.csv'
region = 'CONUS'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

interval_step = 30
max_dis = 1501
out_folder = in_location + os.sep + 'SprayInterval_IntStep_{0}_MaxDistance_{1}'.format(str(interval_step), str(max_dis))
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
dir_folder = os.path.dirname(in_location)

bins = np.arange((0 - interval_step), max_dis, interval_step)
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']

use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'FinalColHeader', 'Type', 'Cell Size']].copy()
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)

list_csv = os.listdir(in_location)
list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

distance_cols = ['0', '30', '42', '60', '67', '84', '90', '94', '108', '120', '123', '127', '134', '150', '152', '161',
                 '169', '174', '180', '182', '189', '192', '201', '210', '212', '216', '218', '228', '234', '240',
                 '241', '247', '254', '256', '258', '268', '270', '271', '276', '283', '284', '295', '296', '300',
                 '301', '305', '308', '313', '318', '323', '324', '330', '331', '335', '339', '342', '349', '351',
                 '360', '361', '362', '364', '366', '371', '375', '379', '381', '384', '390', '391', '394', '400',
                 '402', '403', '408', '416', '417', '420', '421', '424', '426', '429', '432', '436', '442', '445',
                 '450', '453', '456', '457', '458', '465', '466', '468', '469', '474', '480', '483', '484', '488',
                 '492', '494', '496', '499', '502', '509', '510', '512', '513', '516', '517', '523', '524', '530',
                 '531', '534', '536', '540', '543', '547', '550', '551', '553', '558', '560', '563', '566', '569',
                 '570', '573', '576', '577', '579', '582', '589', '590', '591', '593', '595', '597', '600', '602',
                 '603', '606', '607', '611', '615', '617', '618', '624', '626', '630', '632', '635', '636', '637',
                 '641', '642', '644', '646', '647', '648', '655', '657', '658', '660', '662', '664', '666', '670',
                 '674', '676', '678', '680', '684', '685', '690', '692', '695', '697', '699', '700', '702', '706',
                 '708', '711', '713', '715', '720', '721', '722', '724', '725', '726', '729', '730', '732', '735',
                 '737', '740', '742', '745', '750', '751', '752', '755', '757', '758', '759', '763', '764', '766',
                 '768', '771', '774', '778', '780', '782', '785', '787', '789', '792', '794', '797', '798', '800',
                 '804', '806', '807', '810', '812', '814', '816', '818', '819', '823', '825', '827', '829', '831',
                 '833', '834', '835', '836', '840', '842', '844', '845', '846', '848', '849', '852', '853', '858',
                 '859', '863', '865', '870', '872', '873', '874', '876', '878', '882', '885', '886', '888', '890',
                 '891', '894', '898', '900', '901', '902', '904', '906', '907', '910', '912', '913', '914', '915',
                 '917', '918', '920', '924', '926', '930', '931', '933', '934', '937', '939', '941', '942', '947',
                 '948', '952', '953', '954', '956', '957', '958', '960', '961', '964', '966', '967', '968', '969',
                 '971', '973', '975', '976', '977', '979', '980', '982', '984', '986', '989', '990', '991', '993',
                 '994', '997', '998', '999', '1,001', '1,002', '1,005', '1,006', '1,008', '1,012', '1,015', '1,018',
                 '1,019', '1,020', '1,021', '1,023', '1,025', '1,026', '1,027', '1,030', '1,032', '1,033', '1,034',
                 '1,035', '1,036', '1,039', '1,040', '1,041', '1,043', '1,044', '1,046', '1,047', '1,049', '1,050',
                 '1,051', '1,053', '1,055', '1,056', '1,060', '1,061', '1,063', '1,064', '1,065', '1,068', '1,070',
                 '1,072', '1,073', '1,074', '1,075', '1,077', '1,080', '1,081', '1,082', '1,083', '1,084', '1,086',
                 '1,087', '1,090', '1,092', '1,094', '1,098', '1,100', '1,101', '1,103', '1,106', '1,110', '1,111',
                 '1,113', '1,114', '1,116', '1,120', '1,123', '1,124', '1,126', '1,127', '1,129', '1,130', '1,132',
                 '1,134', '1,135', '1,138', '1,140', '1,141', '1,142', '1,143', '1,145', '1,146', '1,148', '1,149',
                 '1,152', '1,154', '1,157', '1,158', '1,159', '1,164', '1,166', '1,167', '1,168', '1,170', '1,171',
                 '1,173', '1,176', '1,178', '1,179', '1,180', '1,181', '1,182', '1,183', '1,186', '1,187', '1,188',
                 '1,189', '1,190', '1,194', '1,195', '1,197', '1,198', '1,200', '1,201', '1,203', '1,204', '1,205',
                 '1,206', '1,207', '1,209', '1,210', '1,213', '1,214', '1,215', '1,218', '1,221', '1,223', '1,224',
                 '1,225', '1,230', '1,231', '1,233', '1,234', '1,235', '1,236', '1,239', '1,240', '1,242', '1,243',
                 '1,244', '1,247', '1,248', '1,250', '1,251', '1,252', '1,253', '1,256', '1,259', '1,260', '1,261',
                 '1,263', '1,264', '1,265', '1,266', '1,268', '1,271', '1,272', '1,273', '1,274', '1,275', '1,276',
                 '1,277', '1,279', '1,281', '1,282', '1,284', '1,288', '1,290', '1,291', '1,292', '1,293', '1,294',
                 '1,295', '1,297', '1,298', '1,299', '1,301', '1,302', '1,303', '1,306', '1,308', '1,309', '1,310',
                 '1,312', '1,314', '1,315', '1,317', '1,318', '1,320', '1,321', '1,323', '1,324', '1,325', '1,326',
                 '1,328', '1,331', '1,332', '1,336', '1,337', '1,339', '1,340', '1,341', '1,343', '1,344', '1,347',
                 '1,348', '1,350', '1,351', '1,352', '1,353', '1,355', '1,356', '1,357', '1,358', '1,359', '1,360',
                 '1,361', '1,363', '1,364', '1,366', '1,368', '1,369', '1,370', '1,371', '1,373', '1,374', '1,376',
                 '1,379', '1,380', '1,381', '1,382', '1,384', '1,385', '1,386', '1,387', '1,388', '1,389', '1,391',
                 '1,392', '1,394', '1,395', '1,397', '1,398', '1,399', '1,400', '1,402', '1,404', '1,405', '1,406',
                 '1,408', '1,410', '1,411', '1,412', '1,413', '1,415', '1,416', '1,417', '1,418', '1,421', '1,422',
                 '1,423', '1,425', '1,426', '1,429', '1,430', '1,431', '1,432', '1,434', '1,435', '1,436', '1,437',
                 '1,440', '1,441', '1,442', '1,443', '1,444', '1,447', '1,448', '1,449', '1,451', '1,452', '1,453',
                 '1,455', '1,456', '1,458', '1,459', '1,461', '1,462', '1,463', '1,464', '1,465', '1,466', '1,467',
                 '1,470', '1,471', '1,472', '1,474', '1,475', '1,476', '1,477', '1,480', '1,481', '1,482', '1,484',
                 '1,485', '1,487', '1,489', '1,490', '1,491', '1,492', '1,493', '1,494', '1,497', '1,499', '1,500']


def roll_up_table(df, dis_cols, use_nm):
    cols = df.columns.values.tolist()
    reindex_col = []
    out_cols = ['EntityID']
    for col in cols:
        if col in dis_cols:
            int_col = col.replace(',', '')
            new_col = "CONUS_" + use_nm + "_" + str(int_col)
            out_cols.append(new_col)
            reindex_col.append(new_col)
        else:
            reindex_col.append(col)
    df.columns = reindex_col
    df = df.reindex(columns=out_cols)
    return df


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(DBF_dir)


def apply_distance_interval(in_df, final_col_value, out_df, type, cell_size, acres_for_calc):
    [in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
    columns_in_df_numeric = [t for t in in_df.columns.values.tolist() if t.split("_")[0] in regions]
    in_df.ix[:, columns_in_df_numeric] = in_df.ix[:, columns_in_df_numeric].apply(pd.to_numeric)

    in_table_w_values = in_df.groupby(['EntityID', ], as_index=False).sum()

    transformed = in_table_w_values.T.reset_index()

    transformed.columns = transformed.iloc[0]
    transformed = transformed.reindex(transformed.index.drop(0))
    update_cols = transformed.columns.values
    update_cols[0] = 'Use_Interval'
    transformed.columns = update_cols

    transformed['Use_Interval'] = transformed['Use_Interval'].map(
        lambda x: str(x).split('_')[len(x.split('_')) - 1] if len(x.split('_')) > 2 else 'NaN').astype(int)
    transformed.ix[:, :] = transformed.ix[:, :].apply(pd.to_numeric)

    bin_labels = bins.tolist()
    bin_labels.remove((0 - interval_step))

    # breaks out into binned intervals and sums
    binned_df = transformed.groupby(pd.cut(transformed['Use_Interval'], bins, labels=bin_labels)).sum()
    binned_df.drop('Use_Interval', axis=1, inplace=True)
    binned_df = binned_df.reset_index()

    group_df_by_zone_sum = binned_df.transpose()  # transposes so it is species by interval and not interval by species

    # Makes the interval values the col header then drops the row with those values
    group_df_by_zone_sum.columns = group_df_by_zone_sum.iloc[0]
    group_df_by_zone_sum = group_df_by_zone_sum.reindex(group_df_by_zone_sum.index.drop('Use_Interval')).reset_index()
    # EntityID was the index, after resetting index EntityID can be added to col headers
    update_cols = group_df_by_zone_sum.columns.values.tolist()
    update_cols[0] = 'EntityID'
    group_df_by_zone_sum.columns = update_cols
    group_df_by_zone_sum['EntityID'] = group_df_by_zone_sum['EntityID'].map(lambda x: x).astype(str)

    # sets up list that will be used to populate the final col header the use name and interval value for
    # non-species cols ie use_nm "_" + interval, then re-assigns col header
    cols = group_df_by_zone_sum.columns.values.tolist()
    out_col = ['EntityID']

    for i in cols:
        if i in out_col:
            pass
        else:
            col = final_col_value + "_" + str(i)
            out_col.append(col)
    group_df_by_zone_sum.columns = out_col

    # merges current use to running acres table to get to percent overlap

    sum_df_acres = pd.merge(group_df_by_zone_sum, acres_for_calc, on='EntityID', how='left')
    sum_df_acres.fillna(0, inplace =True)
    percent_overlap = calculation(type, sum_df_acres, cell_size, 'CONUS', 'regional range')

    return percent_overlap


def calculation(type_fc, in_sum_df, cell_size, c_region, percent_type):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.select_dtypes(include=['number']).columns.values.tolist()
    if percent_type == 'full range':
        acres_col = 'TotalAcresOnLand'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'NL48 range':
        acres_col = 'TotalAcresNL48'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'regional range':
        acres_col = 'Acres_' + str(c_region)
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]

    if type_fc == "Raster":
        msq_conversion = cell_size * cell_size
        # convert pixels to msq
        overlap = in_sum_df.copy()
        overlap.ix[:, use_cols] *= msq_conversion
        # convert msq to acres
        overlap.ix[:, use_cols] *= 0.000247

        # generate percent overlap by taking acres of use divided by total acres of the species range
        overlap[use_cols] = overlap[use_cols].div(overlap[acres_col], axis=0)
        overlap.ix[:, use_cols] *= 100
        # Drop excess acres col- both regional and full range are included on input df; user defined parameter
        # percent_type determines which one is used in overlap calculation
        if percent_type == 'full range':
            overlap.drop('TotalAcresNL48', axis=1, inplace=True)
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)

        elif percent_type == 'NL48 range':
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)

        elif percent_type == 'regional range':
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)
            overlap.drop('TotalAcresNL48', axis=1, inplace=True)

        try:
            overlap.drop(acres_col, inplace= True)
        except:
            pass

        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print "ERROR ERROR"


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)
out_max = base_sp_df.copy()
out_min = base_sp_df.copy()
out_uniform = base_sp_df.copy()
create_directory(out_folder)

acres_df = pd.read_csv(in_acres_table)
acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
for csv in list_csv:
    csv_lookup = csv.replace("_max.csv", ".csv")
    csv_lookup = csv_lookup.replace("_avg.csv", ".csv")

    use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Usage lookup'].iloc[0]
    final_col_v = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'FinalColHeader'].iloc[0]

    type_use = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Type'].iloc[0]
    r_cell_size = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Cell Size'].iloc[0]

    max = pd.read_csv(in_location + os.sep + csv)
    max.ix[:, distance_cols] = max.ix[:, distance_cols].apply(pd.to_numeric)

    max.ix[:,'0'] = max.ix[:,'Max in species range'].map(lambda x: x).astype(float)
    max = roll_up_table(max, distance_cols, use_lookup_df_value)
    per_overlap_interval = apply_distance_interval(max, final_col_v, base_sp_df, type_use, r_cell_size, acres_for_calc)
    out_max = pd.merge(out_max, per_overlap_interval, on='EntityID', how='left')


    min = pd.read_csv(in_location + os.sep + csv)
    min.ix[:,'0'] = min.ix[:,'Min in Species range'].map(lambda x: x).astype(float)
    min = roll_up_table(min, distance_cols, use_lookup_df_value)
    per_overlap_interval_min = apply_distance_interval(min, final_col_v, base_sp_df, type_use, r_cell_size,
                                                       acres_for_calc)
    out_min = pd.merge(out_min, per_overlap_interval_min, on='EntityID', how='left')

    uniform = pd.read_csv(in_location + os.sep + csv)
    uniform.ix[:,'0'] = uniform.ix[:,'Uniform'].map(lambda x: x).astype(float)
    uniform = roll_up_table(uniform, distance_cols, use_lookup_df_value)
    per_overlap_interval_uni = apply_distance_interval(uniform, final_col_v, base_sp_df, type_use, r_cell_size,
                                                       acres_for_calc)
    out_uniform = pd.merge(out_uniform, per_overlap_interval_uni, on='EntityID', how='left')

out_max_csv = out_folder + os.sep + 'Upper' + "_SprayInterval_" + date + '.csv'
out_max.to_csv(out_max_csv)

out_min_csv = out_folder + os.sep + 'Lower' + "_SprayInterval_" + date + '.csv'
out_min.to_csv(out_min_csv)

out_uniform_csv = out_folder + os.sep + 'Uniform' + "_SprayInterval_" + date + '.csv'
out_uniform.to_csv(out_uniform_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
