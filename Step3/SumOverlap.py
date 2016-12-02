import os
import pandas as pd
import datetime

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

infolder = 'J:\Workspace\Step3_Proposal\Refuge\RawRefugeUse\CSV'
##export att table of refuge clips of hab
orgATTacres = 'J:\Workspace\Step3_Proposal\Refuge\RefugeOriginalAttTables\CSV'
outlocation = 'J:\Workspace\Step3_Proposal\Refuge\ExportResults'
completedlist=[]
listfolder = os.listdir(infolder)

speHabitDict = {'94': [286, 38, 97, 90],
                '123': [39, 41, 42, 45, 56, 148, 179, 277, 278, 281, 282, 296, 297, 298, 300, 301, 302, 303, 304, 305,
                        358, 359, 360, 556, 557, 558, 559, 562, 563, 581, 582, 583],
                '133': [290, 291, 292, 293, 294, 295, 363, 370, 379, 407, 408, 410, 411, 449, 556, 557, 574, 575],
                '145': [296, 297, 298, 300, 302, 303, 359, 360, 383, 384, 385, 470, 471, 472, 476, 485, 489],
                '6901': [39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 54, 55, 56, 57, 58, 136, 137, 138, 139, 140, 141,
                         142, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 158, 159, 160, 161, 162,
                         163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 181, 182,
                         183, 184, 185, 186, 187, 188, 189, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276,
                         277, 278, 279, 280, 281, 282, 356, 357, 358, 359, 360, 361, 424, 425, 427, 555],

                '10147': [333, 335, 337, 422, 424, 426],
                '89': [539, 541, 553, 460, 461, 462, 466, 467, 468, 469, 472, 473, 474, 475, 476, 477, 358, 443, 444],
                '139': [30, 46, 50, 51, 52, 53, 59, 65, 70, 91, 104, 188, 193, 197, 198, 200, 222, 223, 235, 240, 253,
                        281, 283, 284],
                '149': [39, 40, 41, 42, 43, 45, 46, 47, 48, 49, 55, 56, 145, 146, 148, 49, 151, 152, 153, 154, 155, 156,
                        158, 159, 162, 163, 164, 165, 175, 179, 181, 183, 184, 185, 186, 187, 188, 189, 194, 266, 270,
                        271, 272, 277, 278, 280, 281, 282, 296, 297, 298, 300, 301, 302, 303, 304, 305, 309, 315, 316,
                        317, 323, 326, 329, 330, 331, 356, 357, 358, 359, 360, 361, 383, 384, 385, 432, 433, 438, 439,
                        442, 443, 444, 445, 455, 457, 458, 459, 509, 578, 579],
                '83': [556, 557, 20, 30, 70, 222, 223, 233, 235, 284, 560, 562, 479, 480, 481, 482, 483, 339, 340, 372,
                       378, 392, 393, 415, 452, 454],
                '2692': [558, 559, 560, 562, 529, 535, 536, 537, 539, 540, 541, 542, 545, 315, 316, 317, 324, 326, 327,
                         328, 329, 330, 331, 338, 356, 358, 426, 438, 439, 442, 443, 445, 457, 459],
                '138': [30, 46, 47, 50, 51, 52, 53, 59, 65, 70, 91, 104, 188, 189, 192, 193, 197, 198, 200, 222, 223,
                        235, 240, 253, 271, 281, 283, 284, 317, 324, 328, 329, 330, 331, 338, 339, 358, 392, 426, 442,
                        459, 460, 461, 462, 463, 464, 465, 467, 468, 469, 475, 479, 481, 482, 483, 556, 557, 558, 559,
                        560],
                '137': [444, 457, 459, 470, 471, 472, 476, 477, 489, 581],
                '116': [303, 304, 385, 432],
                '4064': [315, 316, 317, 323, 326, 438, 439, 502, 503, 556, 558, 559],
                '2691': [558, 559, 560, 562, 529, 535, 536, 537, 539, 540, 541, 542, 545, 315, 316, 317, 324, 326, 327,
                         328, 329, 330, 331, 338, 356, 358, 426, 438, 439, 442, 443, 445, 457, 459]
}

entList = speHabitDict.keys()

agzoneValuesDict = dict(vegGround=[26, 56, 60, 61, 68], orchard=[70], corn=[10, 14, 15, 18],
                        cotton=[20, 25, 26, 42], rice=[30], soybean=[40, 42, 45, 48, 14],
                        wheat=[50, 56, 58, 15, 25, 45], othergrain=[80], otherrow=[90], othercrop=[100],
                        pasture=[110])
nonagzoneValuesDict = dict(CattleEarTag=[1], CullPiles=[1], Developed=[1],
                           Nurseries=[1], OSD=[1], PineSeedOrchards=[1],
                           ROW=[1], XmasTrees=[1])

zoneValuesDict = dict(vegGround=[26, 56, 60, 61, 68], orchard=[70], corn=[10, 14, 15, 18],
                      cotton=[20, 25, 26, 42], rice=[30], soybean=[40, 42, 45, 48, 14],
                      wheat=[50, 56, 58, 15, 25, 45], othergrain=[80], otherrow=[90], othercrop=[100],
                      pasture=[110], CattleEarTag=[1], CullPiles=[1], Developed=[1], Nurseries=[1], OSD=[1],
                      PineSeedOrchards=[1], ROW=[1], XmasTrees=[1])
aglistuse = agzoneValuesDict.keys()
nonaglist = nonagzoneValuesDict.keys()

for folder in listfolder:
    start_species = datetime.datetime.now()

    entid = folder.split("_")
    entid = str(entid[5])
    if entid not in entList:
        continue

    if entid in completedlist:
        continue

    print ('Starting species {0} '.format(entid))
    acrescsvpath = orgATTacres + os.sep + 'natgaplandcov_v2_2_1_clip_' + entid + '.csv'

    dfacrescsv = pd.read_csv(acrescsvpath)
    rowcount = dfacrescsv.count(axis=0, level=None, numeric_only=False)
    rowindex = rowcount.values[0]

    row = 0
    countacres = 0
    while row < rowindex:
        value = dfacrescsv.iloc[row, 1]
        countacres = countacres + value
        row += 1

    msq = countacres * 900
    totalacres = msq * 0.000247
    print "The total acres for is {0} with pixel count {1}".format(totalacres, countacres)

    speHab = speHabitDict[entid]
    perferHabcount = 0

    listhabcode = dfacrescsv['Value'].values.tolist()
    for habcode in speHab:
        rowindex = listhabcode.index(habcode)
        value = dfacrescsv.iloc[rowindex, 1]
        perferHabcount = perferHabcount + value

    msq = perferHabcount * 900
    Perferedacres = msq * 0.000247
    print "The preferred acres for is {0} with pixel count {1}".format(Perferedacres, perferHabcount)

    listcsv = os.listdir(infolder + os.sep + folder)
    results = []
    for csv in listcsv:
        csvpath = infolder + os.sep + folder + os.sep + csv
        parsename = csv.split("_")
        indexsparydrift = len(parsename)
        spraydrift = parsename[(indexsparydrift - 1)]

        if parsename[0] == 'NonAg':
            usecsv = parsename[3]
            year = 'NA'
        elif parsename[0] == 'CONUS':
            usecsv = 'CONUS'
            use = parsename[3]
            year = 'NA'
        elif parsename[0] == 'Clipped':
            usecsv = 'RAW'
            year = str(parsename[3])
        else:
            usecsv = parsename[1]
            year = str(parsename[3])

        if usecsv == 'RAW':
            spraydrift = 'RAW'
            dfcsv = pd.read_csv(csvpath)
            listheader = list(dfcsv.columns.values)
            rowcount = dfcsv.count(axis=0, level=None, numeric_only=False)
            rowindex = rowcount.values[0]

            row = 0
            col = 0
            countpixels = 0

            for use in aglistuse:
                cropvalue = agzoneValuesDict[use]
                countpixels = 0
                for value in speHab:
                    row = value - 1
                    for v in cropvalue:
                        header = 'Value_' + str(v)
                        if header not in listheader:
                            countpixels = countpixels + 0
                        else:
                            colindex = listheader.index(header)
                            pixel = dfcsv.iloc[row, colindex]
                            countpixels = countpixels + pixel

                msq = countpixels * 900
                acres = msq * 0.000247
                percentover = acres / totalacres * 100
                perferedover = acres / Perferedacres * 100

                df2 = [use, year, spraydrift, countpixels, msq, acres, totalacres, percentover, Perferedacres,
                       percentover]
                results.append(df2)
        elif usecsv == 'CONUS':
            spraydrift = 'RAW'
            dfcsv = pd.read_csv(csvpath)
            # print csvpath
            listheader = list(dfcsv.columns.values)
            rowcount = dfcsv.count(axis=0, level=None, numeric_only=False)
            rowindex = rowcount.values[0]
            use = parsename[3]
            use = use.replace('.csv', '')

            row = 0
            col = 0
            countpixels = 0

            cropvalue = nonagzoneValuesDict[use]
            countpixels = 0
            for value in speHab:
                row = value - 1
                for v in cropvalue:
                    header = 'Value_' + str(v)
                    if header not in listheader:
                        countpixels = countpixels + 0
                    else:
                        colindex = listheader.index(header)
                        pixel = dfcsv.iloc[row, colindex]
                        countpixels = countpixels + pixel

            msq = countpixels * 900
            acres = msq * 0.000247
            percentover = acres / totalacres * 100
            perferedover = acres / Perferedacres * 100

            df2 = [use, year, spraydrift, countpixels, msq, acres, totalacres, percentover, Perferedacres,
                   perferedover]
            results.append(df2)



        else:
            if spraydrift == 'buf11.csv':
                spraydrift = 'Ground'
            else:
                spraydrift = 'Aerial'

            dfcsv = pd.read_csv(csvpath)
            listheader = list(dfcsv.columns.values)

            rowcount = dfcsv.count(axis=0, level=None, numeric_only=False)
            rowindex = rowcount.values[0]

            row = 0
            col = 0
            countpixels = 0

            for value in speHab:
                row = value - 1
                cropvalue = zoneValuesDict[usecsv]
                for v in cropvalue:
                    header = 'Value_' + str(v)
                    if header not in listheader:
                        countpixels = countpixels + 0
                    else:
                        colindex = listheader.index(header)
                        # print row
                        # print colindex
                        try:
                            pixel = dfcsv.iloc[row, colindex]
                            # print ("Value for {0} {4} at {1} , {2} is {3}, with buffer {6} heder list {5}".format(usecsv, row,colindex,pixel,year,listheader, spraydrift))
                            countpixels = countpixels + pixel
                        except:
                            print '\n loop up 0 for {0} at {1} , {2}'.format(csvpath, row, colindex)
                            countpixels = countpixels + 0

            msq = countpixels * 900
            acres = msq * 0.000247
            percentover = acres / totalacres * 100
            perferedover = acres / Perferedacres * 100

            df2 = [(str(usecsv)), year, spraydrift, countpixels, msq, acres, totalacres, percentover, Perferedacres,
                   perferedover]
            results.append(df2)

    outDF = pd.DataFrame(results, columns=['Use', 'CDL Year', 'Spray Drift', 'Count', 'MSQ', 'Acres', 'TotalAcres',
                                           'Percent Overlap Total', 'Total Acres Preferred',
                                           'Percent Overlap Preferred'])
    # print outDF
    outfile = 'SummaryFile_' + entid + '.csv'
    outpath = outlocation + os.sep + outfile
    outDF.to_csv(outpath)

    print "Exported species {1} Summary table: {0}".format(datetime.datetime.now() - start_species, entid)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
