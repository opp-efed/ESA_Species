
import os
import pandas as pd
import unicodedata

filelocation = 'C:\\Users\\JConno02\\Documents\\Projects\\ESA\\ReportsFromKeith\\20160620_5-year review report.csv'
df = pd.read_csv(filelocation)
outlocation = 'C:\\Users\\JConno02\\Documents\\Projects\\ESA\\ReportsFromKeith'
outfile = '5_yearReview.csv'

listheader = list(df.columns.values)
count_col = len(listheader)
rowcount = df.count(axis=0, level=None, numeric_only=False)
rowindex = rowcount.values[0]
row = 0
outvalues = []
while row < (rowindex):
    current_species = []
    comname = str(df.iloc[row, 0])
    spname = str(df.iloc[row, 1])
    Currentstatus = str(df.iloc[row, 2])
    Leadregion = str(df.iloc[row, 3])
    Yearlisted = str(df.iloc[row, 4])
    Dateinitiation = str(df.iloc[row, 5])
    Datecompletion = str(df.iloc[row, 6])
    Reportavailable = str(df.iloc[row, 7])



    sciname = spname.replace(comname, '')
    try:

        comname = unicodedata.normalize("NFKD", comname)
    except TypeError:
        pass

    current_species.append(comname)
    current_species.append(sciname)
    current_species.append(Currentstatus)
    current_species.append(Leadregion)
    current_species.append(Yearlisted)
    current_species.append(Dateinitiation)
    current_species.append(Datecompletion)
    current_species.append(Reportavailable)
    outvalues.append(current_species)
    row += 1

outDF = pd.DataFrame(outvalues)
print outDF

outpath = outlocation + os.sep + outfile
outDF.to_csv(outpath)
