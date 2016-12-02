import os
import pandas as pd
import datetime

FileNew = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\Update_Fall2016\ArchivedData\UpdatedBins_20161115.xlsx'
SheetName = 'UpdatedBins_20161115'
indexstart = 8
indexstop = 18
outLocation = 'C:\Users\JConno02\Documents\Projects\ESA\Bins\Update_Fall2016'
outname = 'Recode_BinTable_asof_20161115'

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

outfile = outLocation + os.sep + outname + '.csv'

DBcodeDict = {'1': 'No',
              '2': 'Yes',
              '3': 'Yes/R',
              '4': 'R',
              '5': 'Dummy Bin',
              '6': 'Yes',
              '7': 'Indirect only- Marine host',
              '8': 'Yes- FH-Obligate',
              '9': 'Yes- FH-Generalist',
              '10': 'Yes- FH-Specialist',
              '11': 'Yes- FH-Unknown',
              '28': 'Yes/Yes- FH-Obligate',
              '29': 'Yes/ Yes-Fish Host- Generalist',
              '210': 'Yes/Yes-Fish Host- Specialist',
              '211': 'Yes/Yes- Fish Host- Unknown',
              '12': 'Food item',
              '412': 'Reassigned-Food item',
              '312': 'Food item/Reassigned-Food item',
              '612': 'Food item'}


def decode_value(value):
    try:
        if type(value) == str:
            # Ignore errors even if the string is not proper UTF-8 or has
            # broken marker bytes.
            # Python built-in function unicode() can do this.
            value = unicode(value, "utf-8", errors="ignore")
            return value
        else:
            # Assume the value object has proper __unicode__() method
            value = unicode(str(value))
            return value
    except:
        print type(value)
        value = value.encode('ascii', 'ignore').decode('ascii')
        # value =value.replace("u'\u","'")
        return value


def update_bin_info(bins_df):
    # count_entity = bins_df['ENTITYID'].value_counts()
    entid_df = pd.DataFrame(bins_df['ENTITYID'])
    entid_df.columns = ['EntityID']
    entid_df['count'] = entid_df.groupby('EntityID')['EntityID'].transform('count')
    list_ents = entid_df['EntityID'].values.tolist()
    row_count = len(bins_df)
    row = 0

    while row < row_count:
        entid = bins_df.loc[row, 'ENTITYID']
        row_index_ent = list_ents.index(entid)
        chk_count = entid_df.iloc[row_index_ent, 1]
        if chk_count > 1:
            bins_df.iloc[row, 7] = 'Y'
        else:
            bins_df.iloc[row, 7] = 'N'
        print row
        row += 1
    return bins_df


df_bins = pd.read_excel(FileNew, sheetname=SheetName, header=0, skiprows=0, skip_footer=0, index_col=None,
                        parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                        convert_float=True, has_index_names=None, converters=None, engine=None)

rowcount = df_bins.count(axis=0, level=None, numeric_only=False)
rowindex = rowcount.values[0]

listheader = df_bins.columns.values.tolist()
colcount = len(listheader)
print colcount
outDF = pd.DataFrame(columns=listheader)

col = 0
row = 0

outlist = []
while row <= (rowindex - 1):
    col = 0
    currentlist = []
    while col <= (colcount - 1):
        if col < indexstart or col > indexstop:
            print 'Current index is {0} , {1}'.format(row, col)
            value = df_bins.iloc[row, col]
            outputvalue = decode_value(value)
            if outputvalue.startswith("u"):
                outputvalue = outputvalue.replace("u'\u", "'")
            currentlist.append(outputvalue)
            col += 1
        else:
            print 'Current index is {0} , {1}'.format(row, col)
            value = df_bins.iloc[row, col]
            outputvalue = decode_value(value)
            outputvalue = DBcodeDict[outputvalue]
            if outputvalue.startswith("u"):
                outputvalue = outputvalue.replace("u'\u", "'")
            currentlist.append(outputvalue)
            col += 1

    outlist.append(currentlist)
    row += 1

outDF = pd.DataFrame(outlist, columns=listheader)
outDF = update_bin_info(outDF)
outDF.to_csv(outfile)
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
