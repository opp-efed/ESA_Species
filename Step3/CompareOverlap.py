import os
import pandas as pd

import datetime

inFolder = 'C:\Workspace\ESA_Species\StreamCrosswalk\CSV'

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

#FileOld = 'E:\\test\\old.xlsx'
#FileNew = 'E:\\test\\new.xlsx'

FileOld = 'E:\MasterOverlap\\text\\20160104\\Overlap_20150105.xlsx'
FileNew = 'E:\MasterOverlap\\text\\20160115\\Overlap_20160115.xlsx'
filetype = 'CritHab'
outlocation ='E:\\test\\CHBool_0105_0115.csv'


if filetype== 'Range':
    sheet = 'Collapsed_Range'
else:
    sheet = 'Collapsed_CriticalHabitat'


df_old = pd.read_excel(FileOld, sheetname= sheet, header=0, skiprows=0, skip_footer=0, index_col=None,
                       parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                       convert_float=True, has_index_names=None, converters=None, engine=None)

df_new = pd.read_excel(FileNew, sheetname= sheet, header=0, skiprows=0, skip_footer=0, index_col=None,
                       parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                       convert_float=True, has_index_names=None, converters=None, engine=None)



list = df_new.columns.values.tolist()
outDF = pd.DataFrame(columns=list)

rowcount= df_new.count(axis=0, level=None, numeric_only=False)

rowindex= rowcount.values[0]
colindex = len(list)


print colindex

col = 0
row = 0

while col < (colindex+1):
    try:
        colheader = list[col]
        row = 0
        while row < (rowindex):
            print "Working on row {0}, col {1}".format(row, col)
            try:
                value = df_old.iloc[row, col]
                valueb = df_new.iloc[row, col]
                if value == valueb:
                    result = "TRUE"
                else:
                    result = "FALSE"
            except:
                result = 'OutBounds'
            outDF.set_value(row,colheader,result)
            #outDF.loc[row,colheader] = result

            row = row + 1
    except:
        result = 'OutBounds'
        outDF.set_value(row,colheader,result)
        #outDF.loc[row,colheader] = result
        row = row + 1

    col = col + 1
print outDF

outDF.to_csv(outlocation)
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
