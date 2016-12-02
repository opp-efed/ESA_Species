import pandas as pd
import os
import datetime

inFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables\yearly'
final_folder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables\Collapased'


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
    'Golfcourses':['usa']
}

final_cols = ['HUC12', 'Acres', 'Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat','Vegetables and Ground Fruit',
              'Orchards and Vineyards','Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture', 'Cattle Ear Tag',
              'Developed','Managed Forests', 'Nurseries','Open Space Developed', 'Right of Way', 'CullPiles',
              'Cultivated', 'NonCultivated','Pine seed orchards', 'Christmas Trees','Golfcourses', 'Mosquito Control',
              'Wide Area Use']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

createdirectory(final_folder)
collapse_groups = collapse_dict.keys()
list_csv = os.listdir(inFolder)

for csv in list_csv:
    in_csv = inFolder + os.sep + csv

    out_csv = final_folder + os.sep + csv
    in_df = pd.read_csv(in_csv, dtype=object)
    out_df = in_df[['HUC12', 'Acres_prj_x']]
    out_df.columns = ['HUC12', 'Acres']

    in_df = pd.read_csv(in_csv)
    for group in collapse_groups:
        print group
        current_df = in_df[collapse_dict[group]]
        current_df = current_df.iloc[:, :].apply(pd.to_numeric)

        out_df[group] = current_df.sum(axis=1)
    out_df = out_df.reindex(columns=final_cols)
    out_df['Mosquito Control'] = out_df['Mosquito Control'].map(lambda x: 100)
    out_df['Wide Area Use'] = out_df['Wide Area Use'].map(lambda x: 100)
    out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
