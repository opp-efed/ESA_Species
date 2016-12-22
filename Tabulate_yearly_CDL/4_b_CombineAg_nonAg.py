import pandas as pd
import os
import datetime

in_folder_AG = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\PilotGAP_species\YearlyCDL\PercentOverlap\AllYears'
in_foldernon_NonAg_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\PilotGAP_species\NonAg\PercentOverlap\AllUses\NonAg_GapSpecies_MAG.csv'
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\MagTool\Raw'

out_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'Corn', 'Corn/soybeans', 'Corn/wheat',
           'Corn/grains', 'Cotton', 'Cotton/wheat', 'Cotton/vegetables', 'Rice', 'Soybeans', 'Soybeans/cotton',
           'Soybeans/wheat', 'Soybeans/grains', 'Wheat', 'Wheat/vegetables', 'Wheat/grains',
           'Vegetables and ground fruit', '(ground fruit)', 'Vegetables/grains', 'Orchards and grapes', 'Other trees',
           'Other grains', 'Other row crops', 'Other crops', 'Pasture/hay/forage', 'Developed - open',
           'Developed - low', 'Developed - med', 'Developed - high', 'Forest', 'Shrubland', 'Water', 'Wetlands - woods',
           'Wetlands - herbaceous', 'Miscellaneous land', 'CattleEarTag','Developed','ManagedForests', 'Nurseries',
           'OSD', 'ROW', 'CullPiles','Cultivated', 'NonCultivated','PineSeedOrchards', 'XmasTrees','usa','bermudagrass2',
           'Mosquito Control','Wide Area Use', 'Acres_CONUS']
def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

non_ag_df = pd.read_csv(in_foldernon_NonAg_csv)
non_ag_df.drop('Unnamed: 0', axis=1, inplace=True)
non_ag_df = non_ag_df.fillna(0)

createdirectory(out_location)
list_folder = os.listdir(in_folder_AG)
list_csv = [csv for csv in list_folder if csv.endswith('csv')]
final_df = pd.DataFrame(columns=['EntityID'])

for csv in list_csv:
    print csv
    in_csv = in_folder_AG + os.sep + csv
    csv_use = csv.replace('__', '_')
    out_csv = out_location + os.sep + 'Final_NonAg_' + str(csv)
    in_df = pd.read_csv(in_csv)
    in_df.drop('Unnamed: 0', axis=1, inplace=True)
    out_df = pd.merge(in_df, non_ag_df, on='EntityID', how='outer')
    print out_df
    out_df.reset_index(inplace=True)


    out_df= out_df.reindex(columns=out_col)
    out_df['Mosquito Control'] = out_df['Mosquito Control'].map(lambda x: 100)
    out_df['Wide Area Use'] = out_df['Wide Area Use'].map(lambda x: 100)

    out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
