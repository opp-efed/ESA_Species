import pandas as pd
import os
import datetime
import arcpy
#TODO update hard code to output columns reindex
# TODO update output csv filename so that nonag, aa and aggre ag can be accounted for

# inFolder and in_dbf_folder the folder with the results of the runs, both gdb and csv; outfolder in Tabulated folder
inFolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\HUC12\AAs'
in_dbf_folder = inFolder
outFolder= r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\AAs\HUC12_transposed'
use_index_csv = 5 # location of use in fil names when splitting by _
final_merged_csv = 'Transposed_AllHUC2_AA.csv'
def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

final_df = pd.DataFrame()
list_folder = os.listdir(inFolder)
# by folder in in location, generates a list of csv(s) then transposes do it is HUC12 by use
for folder in list_folder:
    final_huc2 = pd.DataFrame(columns=['HUC12'])
    out_folder = outFolder + os.sep + folder
    createdirectory(out_folder)
    HUC2 = folder.replace('NHDPlus', '_')
    list_csv = os.listdir(inFolder + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    # list_csv = [csv for csv in list_csv if csv.startswith('Albers')]
    for csv in list_csv:
        out_csv = out_folder + os.sep + csv

        split_name = csv.split("_")
        use = split_name[use_index_csv]
        print use
        in_csv = inFolder + os.sep + folder + os.sep + csv
        in_df = pd.read_csv(in_csv, dtype=object)
        try:
            in_df.drop('Unnamed: 0', axis=1, inplace=True)
        except:
            pass
        # extracts just the 0 or row use of the euc raster; this is the first row all cols before being transposed
        in_df = in_df.ix[0:0, ]
        try:
            in_df.drop('OBJECTID', axis=1, inplace=True)
        except:
            pass
        transposed_df = in_df.transpose()
        transposed_df.reset_index(inplace=True)
        transposed_df.columns = ['HUC12', use]
        transposed_df = transposed_df.ix[1:, ]
        transposed_df.to_csv(out_csv)
        final_huc2 = pd.merge(final_huc2, transposed_df, on='HUC12', how='outer')

    # final_huc2  = final_huc2 .reindex(columns=['HUC12', 'CattleEarTag', 'CullPiles', 'Cultivated', 'Developed',
    #                                            'ManagedForests', 'NonCultivated', 'Nurseries', 'OSD','PineSeedOrchards',
    #                                            'ROW', 'XmasTrees', 'bermudagrass2', 'usa'])

    #final_huc2.columns= ['HUC12', 'Other Crops', 'Corn', 'Pasture', 'Cotton','Rice', 'Soybeans', 'Wheat',
    #                     'Vegetables and Ground Fruit','Orchards and Vineyards','Other Grains', 'Other RowCrops']

    final_huc_csv = out_folder + os.sep + 'HUC2'+ HUC2 + '_Transposed.csv'
    final_huc2.to_csv(final_huc_csv)

    final_df = pd.concat([final_df , final_huc2], axis=0)

final_df_csv = outFolder + os.sep + final_merged_csv
final_df.to_csv(final_df_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

print 'Check the duplicate hucs 30701060504 delete duplicate, and  11000060405 sum together, and removed blanks '
