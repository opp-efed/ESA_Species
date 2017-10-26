import datetime

import pandas as pd

# Title- Compare two tables to look for changes

# TODO Save bool directly into df and not a list that outputs to list
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# input and output tables
FileOld = 'C:\Users\JConno02\Documents\Projects\ESA\Bins\SpeciesBins_20160324.xlsx'
FileNew = 'C:\Users\JConno02\Documents\Projects\ESA\Bins\SpeciesBins_20160324.xlsx'

outcsv = 'C:\Users\JConno02\Documents\Projects\ESA\Bins\ChangedBins3.csv'

df_old = pd.read_excel(FileOld, sheetname='Sheet8', header=0, skiprows=0, skip_footer=0, index_col=None,
                       parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                       convert_float=True, has_index_names=None, converters=None, engine=None)

df_new = pd.read_excel(FileNew, sheetname='Bins_20160324', header=0, skiprows=0, skip_footer=0, index_col=None,
                       parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                       convert_float=True, has_index_names=None, converters=None, engine=None)

colindex = 17
rowindex = 2959

list_values = df_new.columns.values.tolist()
outDF = pd.DataFrame(columns=list_values)

row = 0

outlist = []
while row < rowindex:
    currentlist = []
    col = 0
    while col < (colindex + 1):
        colheader = list_values[col]
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

        col += 1
        currentlist.append(result)
    row += 1
    outlist.append(currentlist)

outDF = pd.DataFrame(outlist, columns=list_values)

print outDF

outDF.to_csv(outcsv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
