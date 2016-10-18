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
    'CattleEarTag': ['CattleEarTag'],
    'Corn': ['Corn', 'Corn/soybeans', 'Corn/wheat', 'Corn/grains'],
    'Cotton': ['Cotton', 'Cotton/wheat', 'Cotton/vegetables'],
    'Cultivated': ['Cultivated'],
    'Developed': ['Developed'],
    'NonCultivated': ['NonCultivated'],
    'Nurseries': ['Nurseries'],
    'Orch_Grapes': ['Orchards and grapes'],
    'OSD': ['OSD'],
    'Other Crops': ['Other crops'],
    'Other Grains': ['Other grains'],
    'Other Row Crops': ['Other row crops'],
    'Pasture': ['Pasture/hay/forage'],
    'Rice': ['Rice'],
    'ROW': ['ROW'],
    'Soybeans': ['Soybeans', 'Soybeans/cotton', 'Soybeans/wheat', 'Soybeans/grains'],
    'Veg_Ground_Fruit': ['Vegetables and ground fruit', '(ground fruit)', 'Vegetables/grains'],
    'Wheat': ['Wheat', 'Wheat/vegetables', 'Wheat/grains'],
    'Pine seed orchards': ['PineSeedOrchards'],
    'XmasTree': ['XmasTrees'],
    'ManagedForest': ['ManagedForests'],
    'CullPiles': ['CullPiles']
}

final_cols = ['HUC12', 'Acres', 'Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Veg_Ground_Fruit', 'Orch_Grapes',
              'Other Grains', 'Other Row Crops', 'Other Crops', 'Pasture', 'CattleEarTag', 'Developed', 'ManagedForest',
              'Nurseries', 'OSD', 'ROW', 'CullPiles', 'Cultivated', 'NonCultivated', 'Pine seed orchards', 'XmasTree']

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
    out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
