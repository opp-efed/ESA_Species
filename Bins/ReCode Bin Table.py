import os
import pandas as pd
import datetime

FileNew = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\SpeciesBins_20151118.xlsx'
SheetName = 'Sheet5'
indexstart = 8
indexstop = 18
outLocation = 'C:\Users\JConno02\Documents\Projects\ESA\Bins'
outname = 'Bins_20160324Out'

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

outfile = outLocation + os.sep + outname + '.csv'

DBcodeDict = {'1': ' No',
              '2': ' Yes',
              '3': ' Yes/R',
              '4': ' R',
              '5': ' Dummy Bin',
              '6': ' Yes',
              '7': ' Indirect only- Marine host',
              '8': ' Yes- FH-Obligate',
              '9': ' Yes- FH-Generalist',
              '10': ' Yes- FH-Specialist',
              '11': ' Yes- FH-Unknown',
              '28': ' Yes/Yes- FH-Obligate',
              '29': ' Yes/ Yes-Fish Host- Generalist',
              '210': ' Yes/Yes-Fish Host- Specialist',
              '211': ' Yes/Yes- Fish Host- Unknown',
              '12': ' Indirect only-food item'}

def decode_value (value):
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
        #value =value.replace("u'\u","'")
        return value
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
while row <= (rowindex-1):
    col = 0
    currentlist = []
    while col <= (colcount-1):
        if col < indexstart or col > indexstop:
            print 'Current index is {0} , {1}'.format(row,col)
            value = df_bins.iloc[row, col]
            outputvalue = decode_value(value)
            if outputvalue.startswith ("u"):
                outputvalue =outputvalue.replace("u'\u","'")
            currentlist.append(outputvalue)
            col += 1
        else:
            print 'Current index is {0} , {1}'.format(row,col)
            value = df_bins.iloc[row, col]
            outputvalue = decode_value(value)
            outputvalue = DBcodeDict[outputvalue]
            if outputvalue.startswith ("u"):
                outputvalue =outputvalue.replace("u'\u","'")
            currentlist.append(outputvalue)
            col += 1

    outlist.append(currentlist)
    row +=1

outDF = pd.DataFrame(outlist, columns=listheader)

outDF.to_csv(outfile)
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
