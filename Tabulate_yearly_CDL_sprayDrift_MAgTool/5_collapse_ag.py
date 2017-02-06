import pandas as pd
import os
import datetime

inFolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_CriticalHabitat\MagTool\Raw'
final_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_CriticalHabitat\MagTool\Collapsed'
file_type ='CH_'
in_acres_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\CH_Acres_by_region_20161215.csv'

def createdirectory(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        print "created directory {0}".format(new_dir)


collapse_dict = {
    'Cattle Ear Tag': ['CattleEarTag'],
    'Corn': ['Corn', 'Corn/soybeans', 'Corn/wheat', 'Corn/grains'],
    'Cotton': ['Cotton', 'Cotton/wheat', 'Cotton/vegetables'],
    'Cultivated': ['Cultivated'],
    'Developed': ['Developed'],
    'NonCultivated': ['NonCultivated'],
    'Nurseries': ['Nurseries'],
    'Orchards and Vineyards': ['Orchards and grapes'],
    'Open Space Developed': ['OSD'],
    'Other Crops': ['Other crops'],
    'Other Grains': ['Other grains'],
    'Other RowCrops': ['Other row crops'],
    'Pasture': ['Pasture/hay/forage'],
    'Rice': ['Rice'],
    'Right of Way': ['ROW'],
    'Soybeans': ['Soybeans', 'Soybeans/cotton', 'Soybeans/wheat', 'Soybeans/grains'],
    'Vegetables and Ground Fruit': ['Vegetables and ground fruit', '(ground fruit)', 'Vegetables/grains'],
    'Wheat': ['Wheat', 'Wheat/vegetables', 'Wheat/grains'],
    'Pine seed orchards': ['PineSeedOrchards'],
    'Christmas Trees': ['XmasTrees'],
    'Managed Forests': ['ManagedForests'],
    'CullPiles': ['CullPiles'],
    'Golfcourses':['usa'],
    'Bermuda Grass':['bermudagrass2']
}

final_cols = ['EntityID',	'Group',	'comname',	'sciname',	'status_text',	'Des_CH','CH_GIS','Acres_CONUS', 'Corn',
              'Cotton', 'Rice', 'Soybeans', 'Wheat','Vegetables and Ground Fruit','Orchards and Vineyards',
              'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture', 'Cattle Ear Tag','Developed',
              'Managed Forests', 'Nurseries','Open Space Developed', 'Right of Way', 'CullPiles','Cultivated',
              'NonCultivated','Pine seed orchards', 'Christmas Trees','Golfcourses','Bermuda Grass', 'Mosquito Control',
              'Wide Area Use']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

createdirectory(final_folder)
collapse_groups = collapse_dict.keys()
list_csv = os.listdir(inFolder)
list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

final_df = pd.read_csv(in_acres_table)
final_df['EntityID'] = final_df['EntityID'].map(lambda x: x).astype(str)
final_cols_df = final_df.columns.values.tolist()
for csv in list_csv:
    print csv
    in_csv = inFolder + os.sep + csv
    out_csv = final_folder + os.sep + file_type+csv
    in_df = pd.read_csv(in_csv, dtype=object)
    in_df['EntityID'] = in_df['EntityID'].map(lambda x: x).astype(str)

    in_df_cols = in_df.columns.values.tolist()
    print in_df_cols
    for col in in_df_cols:
        if col =='EntityID':
            pass
        else:
            if col in final_cols_df:
                in_df_cols.remove(col)
            else:
                pass
    print in_df_cols
    in_df= in_df.ix[:,in_df_cols]
    print in_df

    out_df =pd.DataFrame()

    for group in collapse_groups:
        # print group
        out_df['EntityID'] = in_df['EntityID']
        current_df = in_df[collapse_dict[group]]
        current_df = current_df.iloc[:, :].apply(pd.to_numeric)

        out_df[group] = current_df.sum(axis=1)
    print out_df


    out_df = pd.merge(final_df, out_df, on='EntityID', how='outer')
    out_df = out_df.reindex(columns=final_cols)
    out_df['Mosquito Control'] = out_df['Mosquito Control'].map(lambda x: 100)
    out_df['Wide Area Use'] = out_df['Wide Area Use'].map(lambda x: 100)
    out_df = out_df.fillna(0)
    out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

