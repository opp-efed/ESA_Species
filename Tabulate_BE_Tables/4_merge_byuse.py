import pandas as pd
import os
import datetime

in_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\Ag\CriticalHabitat\Mag_Spray\PercentOverlap'
out_location = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\Ag\CriticalHabitat\Mag_Spray\MergeByUse'

# in_folder = r'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat\PercentOverlap'
# out_location = 'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat\MergeByUse'
# col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'Corn', 'Corn/soybeans', 'Corn/wheat',
#            'Corn/grains', 'Cotton', 'Cotton/wheat', 'Cotton/vegetables', 'Rice', 'Soybeans', 'Soybeans/cotton',
#            'Soybeans/wheat', 'Soybeans/grains', 'Wheat', 'Wheat/vegetables', 'Wheat/grains',
#            'Vegetables and ground fruit', '(ground fruit)', 'Vegetables/grains', 'Orchards and grapes', 'Other trees',
#            'Other grains', 'Other row crops', 'Other crops', 'Pasture/hay/forage', 'Developed - open',
#            'Developed - low', 'Developed - med', 'Developed - high', 'Forest', 'Shrubland', 'Water', 'Wetlands - woods',
#            'Wetlands - herbaceous', 'Miscellaneous land', 'acres']

def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
createdirectory(out_location)
list_folder = os.listdir(in_folder)

for folder in list_folder:
    print folder
    list_csv = os.listdir(in_folder + os.sep + folder)
    out_df = pd.DataFrame()
    for csv in list_csv:
        print csv

        out_csv = out_location + os.sep + 'Merge_' + str(folder) + '.csv'
        current_csv = in_folder + os.sep + folder + os.sep + csv
        in_df = pd.read_csv(current_csv, dtype=object)
        out_df = pd.concat([out_df, in_df], axis=0)
    # out_df= out_df.reindex(columns=col)
    out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
