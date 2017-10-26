import pandas as pd
import datetime

in_range_table = 'E:\Tabulated_NewComps\FinalBETables\DraftNewFormat\R_FinalBE_DiazinonOverlap_20170109.csv'
in_ch_table = 'E:\Tabulated_NewComps\FinalBETables\DraftNewFormat\CH_FinalBE_DiazinonOverlap_20170109.csv'
out_csv = 'E:\Tabulated_NewComps\FinalBETables\DraftNewFormat\Diazinon_Mismatched_EDcall_20170109.csv'
sp_index_cols = 15

col_list = ['EntityID', 'comname_x', 'sciname_x', 'family_x', 'status_text_x', 'pop_abbrev_x',
            'Group_x', 'Des_CH_x', 'Critical_Habitat__x', 'CH_GIS_x', 'Migratory_x',
            'Range_Overlap_Result', 'Range_Overlap_Result_Comment', 'CH_Overlap_Result', 'CH_Overlap_Result_Comment',
            'Compare R_CH Result','R Comment','CH Comment']


def add_result_final_table(row, column):
    return row[column]


def check_mismatched_eds(row):
    if row['Range_Overlap_Result'] != row['CH_Overlap_Result']:
        if row['Des_CH'] == 'TRUE':
            return 'Mismatched'
        else:
            return 'No CritHab'
    else:
        return 'Same'
def range_comment_eds(row):
    if row['Range_Overlap_Result'] != row['CH_Overlap_Result']:
        if row['Des_CH'] == 'TRUE':
            return 'Range is {0} and CritHab is {1} '.format(row['Range_Overlap_Result'],row['CH_Overlap_Result'])
        else:
            return 'No CritHab'
    else:
        return 'Same'
def ch_comment_eds(row):
    if row['Range_Overlap_Result'] != row['CH_Overlap_Result']:
        if row['Des_CH'] == 'TRUE':
            return 'CritHab is {0} and Range is {1} '.format(row['CH_Overlap_Result'],row['Range_Overlap_Result'],)
        else:
            return 'No CritHab'
    else:
        return 'Same'



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

range_table_df = pd.read_csv(in_range_table)
ch_table_df = pd.read_csv(in_ch_table)

sp_info_df = range_table_df.iloc[:, :sp_index_cols]
use_df = range_table_df.iloc[:, sp_index_cols:]
# print use_df

collapsed_df = pd.DataFrame(data=sp_info_df)

collapsed_df = pd.merge(collapsed_df, range_table_df, on='EntityID', how='outer')
collapsed_df = pd.merge(collapsed_df, ch_table_df, on='EntityID', how='outer')

print collapsed_df.columns.values.tolist()
collapsed_df['Range_Overlap_Result'] =collapsed_df.apply(lambda row: add_result_final_table(row,'Step 2 ED_x'), axis=1)
collapsed_df['Range_Overlap_Result_Comment'] =collapsed_df.apply(lambda row: add_result_final_table(row,'Step 2 ED Comment_x'), axis=1)
collapsed_df['CH_Overlap_Result'] =collapsed_df.apply(lambda row: add_result_final_table(row,'Step 2 ED_y'), axis=1)
collapsed_df['CH_Overlap_Result_Comment'] =collapsed_df.apply(lambda row: add_result_final_table(row,'Step 2 ED Comment_y'), axis=1)

collapsed_df['Compare R_CH Result'] = collapsed_df.apply(lambda row: check_mismatched_eds(row), axis=1)
collapsed_df['R Comment'] = collapsed_df.apply(lambda row: range_comment_eds(row), axis=1)
collapsed_df['CH Comment'] = collapsed_df.apply(lambda row: ch_comment_eds(row), axis=1)
final_df = collapsed_df.reindex(columns=col_list)
print final_df.columns.values.tolist()

final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
