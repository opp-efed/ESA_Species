import os
import pandas as pd
import datetime

# TODO check dynamic assignment of reassingment
FileNew = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived\UpdatedBins_20170503.csv'
outLocation = 'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived'
supporting_col_index = 8
trailing_col = 20
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

outname = 'Recode_BinTable_asof_' + date


def check_reassigned_bin(row, df):
    bool_reassign = 'FALSE'
    reassigned = [3.0, 4.0, 412.0, 312.0]
    ent_huc = row['Entid_HUC']
    lookup_huc_bins = df.loc[df['Entid_HUC'] == ent_huc]
    lookup_huc_bins = lookup_huc_bins.loc[:, ['Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8',
                                              'Bin 9', 'Bin 10']]
    try:
        lookup_huc_bins = lookup_huc_bins.values.tolist()[0]
        for z in lookup_huc_bins:
            if z in reassigned:
                bool_reassign = 'TRUE'
            else:
                pass

        return bool_reassign
    except IndexError:
        return 'Check'


def check_multi_huc(row, df):
    entid = row['ENTITYID']

    filter_df = df.loc[df['ENTITYID'] == entid]
    if len(filter_df) > 1:
        return 'Y'
    elif len(filter_df) == 1:
        return 'N'
    else:
        'Check'


def check_sur_huc(row):
    huc_2 = row['HUC_2']
    huc_2 = int(huc_2[:2])
    if huc_2 >= 22:
        return 'TRUE'
    elif huc_2 <= 21:
        return 'FALSE'
    else:
        return 'Check'


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

outfile = outLocation + os.sep + outname + '.csv'

DBcodeDict = {1: 'No',
              2: 'Yes',
              3: 'Yes/R',
              4: 'R',
              5: 'Dummy Bin',
              6: 'Yes',
              7: 'Indirect only- Marine host',
              8: 'Yes- FH-Obligate',
              9: 'Yes- FH-Generalist',
              10: 'Yes- FH-Specialist',
              11: 'Yes- FH-Unknown',
              12: 'Food item',
              13: 'No',
              28: 'Yes/Yes- FH-Obligate',
              29: 'Yes/ Yes-Fish Host- Generalist',
              137: 'No',
              210: 'Yes/Yes-Fish Host- Specialist',
              211: 'Yes/Yes- Fish Host- Unknown',
              412: 'Reassigned-Food item',
              312: 'Food item/Reassigned-Food item',
              612: 'Food item',
              1312: 'No',
              132: 'No',
              136: 'No',
              1328: 'No',
              1329: 'No',
              13210: 'No',
              13211: 'No'}

# Everything that started with 13 is a code that cannot exists in land-locked HUCS, so it is change to No; but that
# assignment could be meaning for other HUCs


df_bins = pd.read_csv(FileNew, )
[df_bins.drop(v, axis=1, inplace=True) for v in df_bins.columns.values.tolist() if v.startswith('Unnamed')]
df_bins['Entid_HUC'] = df_bins['ENTITYID'] + "_" + df_bins['HUC_2']
df_bins['Reassigned'] = df_bins.apply(lambda row: check_reassigned_bin(row, df_bins), axis=1)

leading_col = df_bins.iloc[:, :supporting_col_index]
bins = df_bins.iloc[:, supporting_col_index:trailing_col]
trailing_col = df_bins.iloc[:, trailing_col:]
bins = bins.replace({.0: ''})
keys = DBcodeDict.keys()

for i in keys:
    value = (DBcodeDict[i])
    bins = bins.replace({i: value})
    print "Replaced {0} to {1}".format(i, value)

df_final = pd.concat([leading_col, bins], axis=1)
df_final = pd.concat([df_final, trailing_col], axis=1)

df_final['Multi HUC'] = df_final.apply(lambda row: check_multi_huc(row, df_final), axis=1)
df_final['sur_huc'] = df_final.apply(lambda row: check_sur_huc(row), axis=1)
df_final.loc[df_final['sur_huc'] == 'TRUE', 'HUC_2'] = '21'

[df_final.drop(v, axis=1, inplace=True) for v in ['Entid_HUC', 'Spe_HUC']]
df_final.drop_duplicates(inplace=True)
df_final.to_csv(outfile)
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
