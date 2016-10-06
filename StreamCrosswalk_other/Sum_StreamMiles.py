import os
import pandas as pd

import datetime

#inFolder = 'C:\Workspace\DD_Species_StreamCross_UpdateMarch2016\CSVExport'
masterlist =r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\FinalLists\FinalBE_December2016\csv\MasterListESA_June2016_20160909.csv'
inFolder = 'C:\Workspace\FinalBE_EucDis_CoOccur\SteamCrosswalk\All_species_inaBin_Master_20160819\CSV'
outlocation = 'C:\Workspace\FinalBE_EucDis_CoOccur\SteamCrosswalk\StreamMilesByHUC12'
# outlocation ='C:\Workspace\ESA_Species\StreamCrosswalk'
outfile = 'StreamMilesHUCDDOnly_OrginalStreamsALL_2' + '.csv'
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Range_Filename', 'Des_CH', 'CH_GIS',
                'CH_Filename']
start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

filelist = os.listdir(inFolder)

results = []
DDspecies = ['273','8278','4881','4411']
for value in filelist:
    print '\n {0}'.format(value)
    CHcheck = value[:2]
    if CHcheck == 'CH':
        ent = value.split("_")
        ent = str(ent[2])
        ent = ent.strip('.csv')
        if ent not in DDspecies:
            continue
        filetype = 'Critical Habitat'
        CHStream = pd.read_csv((inFolder + os.sep + value))
        rowcount = CHStream.count(axis=0, level=None, numeric_only=False)
        rowindex = ((rowcount.values[0]))
        print rowindex

        row = 0
        countacres = 0
        HUCdictkm = {}
        HUCdictmiles = {}
        totalkm = 0

        while row < rowindex:
            reach = int(CHStream.iloc[row, 6])
            reach = str(reach)

            if len(reach) == 13:
                #HUC = reach[0:1]# HUC2
                HUC = reach[0:6]
                print HUC
                listHUCs = HUCdictkm.keys()
                if HUC not in listHUCs:
                    HUCdictkm[HUC] = 0
                    print 'updating dict'

            elif len(reach) == 14:
                #HUC = reach[0:2] #HUC2
                HUC = reach[0:7]
                print HUC

                listHUCs = HUCdictkm.keys()
                if HUC not in listHUCs:
                    HUCdictkm[HUC] = 0
                    print 'updating dict'
            else:
                print "ERROR ERROR reach has {0} for species {1} at row {2}".format(len(reach), ent, row)

            streamkm = float(CHStream.iloc[row, 5])
            # print streamkm
            totalkm = HUCdictkm[HUC]
            newtotalkm = totalkm + streamkm
            HUCdictkm[HUC] = newtotalkm
            row += 1

        try:
            streamkm = CHStream['LENGTHKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} hsd total stream KMs of {1}".format(ent, streamkm)

        except:
            streamkm = CHStream['LengthKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} hsd total stream KMs of {1}".format(ent, streamkm)
    else:
        ent = value.split("_")
        ent = str(ent[2])
        ent = ent.strip('.csv')
        if ent not in DDspecies:
            continue
        filetype = 'Range'
        RStream = pd.read_csv((inFolder + os.sep + value))
        rowcount = RStream.count(axis=0, level=None, numeric_only=False)
        rowindex = (rowcount.values[0])

        row = 0
        countacres = 0
        HUCdictkm = {}
        HUCdictmiles = {}

        while row < rowindex:

            reach = int(RStream.iloc[row, 6])
            reach = str(reach)

            if len(reach) == 13:
                #HUC = reach[0:1]#HUC2
                HUC = reach[0:6]  # HUC8
                print HUC

                listHUCs = HUCdictkm.keys()
                if HUC not in listHUCs:
                    HUCdictkm[HUC] = 0
                    print 'updating dict'

            elif len(reach) == 14:
                #HUC = reach[0:2] #HUC2
                HUC = reach[0:7]
                print HUC
                listHUCs = HUCdictkm.keys()
                if HUC not in listHUCs:
                    HUCdictkm[HUC] = 0
                    print 'updating dict'
            else:
                print "ERROR ERROR reach has {0} for species {1} at row {2}".format(len(reach), ent, row)

            streamkm = float(RStream.iloc[row, 5])
            # print streamkm
            totalkm = HUCdictkm[HUC]
            newtotalkm = totalkm + streamkm
            HUCdictkm[HUC] = newtotalkm
            row += 1

        try:
            streamkm = RStream['LENGTHKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} hsd total stream KMs of {1}".format(ent, streamkm)

        except:
            streamkm = RStream['LengthKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} hsd total stream KMs of {1}".format(ent, streamkm)

    listHUCs = HUCdictkm.keys()

    for value in listHUCs:
        HUC = value
        KMHUC = HUCdictkm[value]
        milesHUC = KMHUC * 0.621
        FullKM = streamkm
        Fullmiles = stream_miles

        listresult = [ent, filetype, HUC, KMHUC, milesHUC, FullKM, Fullmiles]
        print listresult
        results.append(listresult)
outheader =['EntityID','filetype','HUC','KMHUC', 'MilesHUC', 'FullKM', 'FullMiles']
outDF = pd.DataFrame(results, columns=outheader)
# print outDF

if masterlist.split('.')[1]== 'csv':
    master_list_df = pd.read_csv(masterlist)
else:
    master_list_df = pd.read_excel(masterlist)
master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
sp_info_df = pd.DataFrame(master_list_df, columns=col_included)
sp_info_included = sp_info_df[sp_info_df['EntityID'].isin(outDF['EntityID']) == True]
df_final = pd.merge(sp_info_included, outDF, on='EntityID', how='left')

outpath = outlocation + os.sep + outfile
df_final.to_csv(outpath)

# outDF = outDF.append({'EntityID': ent, 'FileType': filetype, 'TotalStreamKM': streamkm, 'TotalStreamMiles': stream_miles},
# ignore_index=True)



print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
