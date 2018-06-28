import pandas as pd
import os

chemical_name = 'Diazinon'
st_cnty = 'States'  # if running on cnty change to Counties
# use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
#              r'\SupportingTables' + os.sep + chemical_name + "_RangeUses_lookup.csv"

use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\Carbaryl_Uses_lookup_20180430.csv'

state_fp_lookup = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentSupportingTables\Usage\ForOverlap\STATEFP_lookup.csv'
pct_table = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
            r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\PCT\Carbaryl\Carbaryl_min_test.csv'

out_location = r'L:\ESA\Tabulates_Usage'
in_location_species = r'L:\ESA\Tabulated_PolBoundaries\L48\Range\Agg_Layers'
in_locations_states = 'L:\ESA\Tabulated_PolBoundaries\PoliticalBoundaries\Agg_Layers\States'

# state_df = pd.read_csv(r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects"
#                        r"\ESA\_ED_results\Tabulated_Jan2018\PolticalBoundaries\PolticalBoundaries\Agg_Layers\States")
# species_df = pd.read_csv(
#     r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Tabulated_Jan2018\PolticalBoundaries\PolticalBoundaries\Agg_Layers\States\CONUS_CDL_1016_10x2_euc.csv")


def state_pct(row, pixel_state_col, pct_col):

    adjust_value = row[pixel_state_col] * row[pct_col]
    return adjust_value

def outside_range(row, state, species_state):

    adjust_value = row[state] - row[species_state]
    return adjust_value

def min_range(row, total, adjust):
    total_outside = row[total]
    st_adjust = row[adjust]

    if total_outside > st_adjust:
        return 0
    else:
        return st_adjust - total_outside


def max_range(row, total, adjust, ):
    st_adjust = row[adjust]
    sp_total = row[total]
    if sp_total < st_adjust:
        return sp_total
    else:
        return st_adjust

def uniform_spe (row, pct, sp_total_col):
    return row[sp_total_col] * row[pct]

def adjust_drift (df, pct):
    # Distance 0 adjusted separately
    # cols = ['30', '42', '60', '67', '84', '90', '94', '108', '120', '123', '127', '134', '150', '152', '161',
    #         '169', '174', '180', '182', '189', '192', '201', '210', '212', '216', '218', '228', '234', '240', '241',
    #         '247', '254', '256', '258', '268', '270', '271', '276', '283', '284', '295', '296', '300', '301', '305',
    #         '308', '313', '318', '323', '324', '330', '331', '335', '339', '342', '349', '351', '360', '361', '362',
    #         '364', '366', '371', '375', '379', '381', '384', '390', '391', '394', '400', '402', '403', '408', '416',
    #         '417', '420', '421', '424', '426', '429', '432', '436', '442', '445', '450', '453', '456', '457', '458',
    #         '465', '466', '468', '469', '474', '480', '483', '484', '488', '492', '494', '496', '499', '502', '509',
    #         '510', '512', '513', '516', '517', '523', '524', '530', '531', '534', '536', '540', '543', '547', '550',
    #         '551', '553', '558', '560', '563', '566', '569', '570', '573', '576', '577', '579', '582', '589', '590',
    #         '591', '593', '595', '597', '600', '602', '603', '606', '607', '611', '615', '617', '618', '624', '626',
    #         '630', '632', '635', '636', '637', '641', '642', '644', '646', '647', '648', '655', '657', '658', '660',
    #         '662', '664', '666', '670', '674', '676', '678', '680', '684', '685', '690', '692', '695', '697', '699',
    #         '700', '702', '706', '708', '711', '713', '715', '720', '721', '722', '724', '725', '726', '729', '730',
    #         '732', '735', '737', '740', '742', '745', '750', '751', '752', '755', '757', '758', '759', '763', '764',
    #         '766', '768', '771', '774', '778', '780', '782', '785', '787', '789', '792', '794', '797', '798', '800',
    #         '804', '806', '807', '810', '812', '814', '816', '818', '819', '823', '825', '827', '829', '831', '833',
    #         '834', '835', '836', '840', '842', '844', '845', '846', '848', '849', '852', '853', '858', '859', '863',
    #         '865', '870', '872', '873', '874', '876', '878', '882', '885', '886', '888', '890', '891', '894', '898',
    #         '900', '901', '902', '904', '906', '907', '910', '912', '913', '914', '915', '917', '918', '920', '924',
    #         '926', '930', '931', '933', '934', '937', '939', '941', '942', '947', '948', '952', '953', '954', '956',
    #         '957', '958', '960', '961', '964', '966', '967', '968', '969', '971', '973', '975', '976', '977', '979',
    #         '980', '982', '984', '986', '989', '990', '991', '993', '994', '997', '998', '999', '1,001', '1,002',
    #         '1,005', '1,006', '1,008', '1,012', '1,015', '1,018', '1,019', '1,020', '1,021', '1,023', '1,025', '1,026',
    #         '1,027', '1,030', '1,032', '1,033', '1,034', '1,035', '1,036', '1,039', '1,040', '1,041', '1,043', '1,044',
    #         '1,046', '1,047', '1,049', '1,050', '1,051', '1,053', '1,055', '1,056', '1,060', '1,061', '1,063', '1,064',
    #         '1,065', '1,068', '1,070', '1,072', '1,073', '1,074', '1,075', '1,077', '1,080', '1,081', '1,082', '1,083',
    #         '1,084', '1,086', '1,087', '1,090', '1,092', '1,094', '1,098', '1,100', '1,101', '1,103', '1,106', '1,110',
    #         '1,111', '1,113', '1,114', '1,116', '1,120', '1,123', '1,124', '1,126', '1,127', '1,129', '1,130', '1,132',
    #         '1,134', '1,135', '1,138', '1,140', '1,141', '1,142', '1,143', '1,145', '1,146', '1,148', '1,149', '1,152',
    #         '1,154', '1,157', '1,158', '1,159', '1,164', '1,166', '1,167', '1,168', '1,170', '1,171', '1,173', '1,176',
    #         '1,178', '1,179', '1,180', '1,181', '1,182', '1,183', '1,186', '1,187', '1,188', '1,189', '1,190', '1,194',
    #         '1,195', '1,197', '1,198', '1,200', '1,201', '1,203', '1,204', '1,205', '1,206', '1,207', '1,209', '1,210',
    #         '1,213', '1,214', '1,215', '1,218', '1,221', '1,223', '1,224', '1,225', '1,230', '1,231', '1,233', '1,234',
    #         '1,235', '1,236', '1,239', '1,240', '1,242', '1,243', '1,244', '1,247', '1,248', '1,250', '1,251', '1,252',
    #         '1,253', '1,256', '1,259', '1,260', '1,261', '1,263', '1,264', '1,265', '1,266', '1,268', '1,271', '1,272',
    #         '1,273', '1,274', '1,275', '1,276', '1,277', '1,279', '1,281', '1,282', '1,284', '1,288', '1,290', '1,291',
    #         '1,292', '1,293', '1,294', '1,295', '1,297', '1,298', '1,299', '1,301', '1,302', '1,303', '1,306', '1,308',
    #         '1,309', '1,310', '1,312', '1,314', '1,315', '1,317', '1,318', '1,320', '1,321', '1,323', '1,324', '1,325',
    #         '1,326', '1,328', '1,331', '1,332', '1,336', '1,337', '1,339', '1,340', '1,341', '1,343', '1,344', '1,347',
    #         '1,348', '1,350', '1,351', '1,352', '1,353', '1,355', '1,356', '1,357', '1,358', '1,359', '1,360', '1,361',
    #         '1,363', '1,364', '1,366', '1,368', '1,369', '1,370', '1,371', '1,373', '1,374', '1,376', '1,379', '1,380',
    #         '1,381', '1,382', '1,384', '1,385', '1,386', '1,387', '1,388', '1,389', '1,391', '1,392', '1,394', '1,395',
    #         '1,397', '1,398', '1,399', '1,400', '1,402', '1,404', '1,405', '1,406', '1,408', '1,410', '1,411', '1,412',
    #         '1,413', '1,415', '1,416', '1,417', '1,418', '1,421', '1,422', '1,423', '1,425', '1,426', '1,429', '1,430',
    #         '1,431', '1,432', '1,434', '1,435', '1,436', '1,437', '1,440', '1,441', '1,442', '1,443', '1,444', '1,447',
    #         '1,448', '1,449', '1,451', '1,452', '1,453', '1,455', '1,456', '1,458', '1,459', '1,461', '1,462', '1,463',
    #         '1,464', '1,465', '1,466', '1,467', '1,470', '1,471', '1,472', '1,474', '1,475', '1,476', '1,477', '1,480',
    #         '1,481', '1,482', '1,484', '1,485', '1,487', '1,489', '1,490', '1,491', '1,492', '1,493', '1,494', '1,497',
    #         '1,499', '1,500']
    cols = [v for v in df.columns.values.tolist() if v.startswith("VALUE_")]
    df.ix[:,cols] = df.ix[:, cols].apply(pd.to_numeric, errors='coerce')
    df.ix[:,cols] = df.ix[:,cols].multiply(df[pct], axis=0)
    return df
# ##
state_fp = pd.read_csv(state_fp_lookup)
state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
pct_df = pd.read_csv(pct_table)

use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup']]
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)
# assumes identifier is in this postion
out_path = out_location +os.sep+chemical_name+os.sep+os.path.basename(pct_table).split("_")[1]

if not os.path.exists(os.path.dirname(out_path)):
    os.mkdir(os.path.dirname(out_path))
if not os.path.exists(out_path):
    os.mkdir(out_path)

t_pct = pct_df.T
t_pct = t_pct.reset_index()
update_cols = t_pct.iloc[0].values.tolist()
update_cols[0] = 'STATE'
t_pct.columns = update_cols
t_pct = t_pct.reindex(t_pct.index.drop(0))
t_pct = pd.merge(t_pct, state_fp, on='STATE', how='left')

list_folders = os.listdir(in_location_species)

for folder in list_folders:
    # in_results_sp = in_location_species + os.sep +folder+ os.sep + st_cnty
    list_csv = os.listdir(in_location_species + os.sep + folder + os.sep + st_cnty)
    print list_csv
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
    csv = list_csv[0]
    if len(list_csv) == 0:
        continue

    # Load state overlap result for state # NOTE file names must be the same as species filter to just direct overlap
    # and other important cols
    csv = list_csv[0]
    state_df = pd.read_csv(in_locations_states + os.sep + folder + '.csv')
    state_df['STATEFP'] = state_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
    filtered_state = state_df.ix[:, ['STATEFP',  'Acres', '0']]
    filtered_state.columns = ['STATEFP', 'Acres', 'State direct pixels']


    # Load species overlap result for state
    species_df = pd.read_csv(in_location_species + os.sep + folder + os.sep + st_cnty)
    species_df['STATEFP'] = species_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(
        str)

    # determine the crop in pct table that applies to the current csv then filter the pct table to just that crop and
    # other columns need to merge

    use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv), 'Usage lookup'].iloc[0]
    filter_col = ['STATE', 'STATEFP']
    filter_col.append(use_lookup_df_value)
    filtered_pct = t_pct.ix[:, filter_col]

    filtered_pct.columns = ['STATE', 'STATEFP', 'PCT_'+use_lookup_df_value]

    # merge the pct to species_df to pct then merge to filtereed state
    merged_species = pd.merge(species_df, filtered_pct, on='STATEFP', how='left')
    merged_species_state = pd.merge(merged_species, filtered_state, on='STATEFP', how='left')
    merged_species_state['Drift_PCT'] =  merged_species_state['PCT_'+use_lookup_df_value].map(lambda x: 0 if x == 0 else 1)

    # PCT Adjustments
    merged_species_state['State pixels adjusted by PCT'] = \
        merged_species_state.apply(lambda row: state_pct(row, 'State direct pixels', 'PCT_'+use_lookup_df_value), axis=1)
    merged_species_state['Total outside species range'] = \
        merged_species_state.apply(lambda row: outside_range(row, 'State direct pixels', '0'), axis=1)
    merged_species_state['Total outside species range'] = \
        merged_species_state.apply(lambda row: outside_range(row, 'State direct pixels', '0'), axis=1)
    merged_species_state['Total outside species range'] = \
        merged_species_state.apply(lambda row: outside_range(row, 'State direct pixels', '0'), axis=1)
    merged_species_state['Min in Species range'] = \
        merged_species_state.apply(lambda row: min_range(row, 'Total outside species range', 'State pixels adjusted by PCT'), axis=1)
    merged_species_state['Max in species range'] = merged_species_state.apply(lambda row: max_range(row, '0', 'State pixels adjusted by PCT'), axis=1)
    merged_species_state['Uniform'] = merged_species_state.apply(lambda row: uniform_spe (row, 'PCT_'+use_lookup_df_value, '0'), axis=1)
    merged_species_state = adjust_drift (merged_species_state, 'Drift_PCT')
    csv_out = csv.replace('.csv',"_"+os.path.basename(pct_table).split("_")[1]+'.csv')
    print csv_out
    merged_species_state.to_csv(out_path+os.sep+csv_out)
    print 'Table can be found at {0}'.format(out_path+os.sep+csv_out)
