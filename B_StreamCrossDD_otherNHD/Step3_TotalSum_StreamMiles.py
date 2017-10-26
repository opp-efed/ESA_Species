import os
import pandas as pd

import datetime
masterlist ='C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_201601221.xlsx'

#inFolder = 'C:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\SteamCrosswalk\Aqu_species_MasterList20160819\CSV'
inFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\DD_streamSummary\CSV'
outlocation ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\DD_streamSummary'
outfile = 'All_DD_Species_20170103' + '.csv'

col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Range_Filename', 'Des_CH', 'CH_GIS',
                'CH_Filename']
start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

filelist = os.listdir(inFolder)
outDF = pd.DataFrame(columns=['EntityID', 'FileType', 'TotalStreamKM', 'TotalStreamMiles'])
results = []

for value in filelist:
    print '\n {0}'.format( value)
    CHcheck = value[:2]
    if CHcheck == 'CH':
        ent = value.split("_")
        ent = str(ent[2])
        ent = ent.strip('.csv')
        filetype = 'Critical Habitat'
        CHStream = pd.read_csv((inFolder + os.sep + value))

        try:
            streamkm = CHStream['LENGTHKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} has total stream KMs of {1}".format(ent, streamkm)

        except:
            streamkm = CHStream['LengthKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} has total stream KMs of {1}".format(ent, streamkm)
    else:
        ent = value.split("_")
        ent = str(ent[2])
        ent = ent.strip('.csv')
        filetype = 'Range'
        RStream = pd.read_csv((inFolder + os.sep + value))

        try:
            streamkm = RStream['LENGTHKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} has total stream KMs of {1}".format(ent, streamkm)

        except:
            streamkm = RStream['LengthKM'].sum()
            stream_miles = streamkm * 0.621
            print "Species {0} has total stream KMs of {1}".format(ent, streamkm)


    outDF = outDF.append({'EntityID': ent, 'FileType': filetype, 'TotalStreamKM': streamkm, 'TotalStreamMiles': stream_miles},
                         ignore_index=True)


print outDF
master_list_df = pd.read_excel(masterlist)
master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
sp_info_df = pd.DataFrame(master_list_df, columns=col_included)
sp_info_included = sp_info_df[sp_info_df['EntityID'].isin(outDF['EntityID']) == True]
df_final = pd.merge(sp_info_included, outDF, on='EntityID', how='left')

outpath = outlocation + os.sep + outfile
df_final.to_csv(outpath)




print "Script completed in: {0}".format(datetime.datetime.now() - start_script)