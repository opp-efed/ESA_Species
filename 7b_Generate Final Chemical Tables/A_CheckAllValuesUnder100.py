import pandas as pd
import os
import datetime


def main():
    path = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Methomyl\Summarized Tables'
    # path = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\Methomyl\Summarized Tables'

    none_use_cols = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48', 'Step 2 ED Comment','Step 1 ED Comment', 'Acres',
                     'State direct msq','Drift_PCT','State msq adjusted by PCT','Total outside species range',
                     'Min in Species range','Max in species range','Uniform','STATEFP','STATE', ]
    list_files = recursive_file_lookup(path)
    check_max_value(list_files, none_use_cols)


def recursive_file_lookup (path):
    files_list =[]
    for root, dirs, files in os.walk(path):
        for name in files:
            if os.path.join(root, name).endswith('.csv'):
                files_list.append(os.path.join(root, name))
        # for name in dirs:
        #     print(os.path.join(root, name))
    return files_list

def check_max_value (list_csvs, none_use):
    for csv in list_csvs:
        if not (os.path.basename(csv)).startswith('Parameters'):

            df = pd.read_csv (csv)
            drop_cols = [col for col in df.columns.values.tolist() if col.startswith('Unnamed')]
            df.drop (drop_cols, axis = 1, inplace = True)

            use_cols = [col for col in df.columns.values.tolist() if col not in none_use]
            # print use_cols
            use_df = df[use_cols]
            use_df.replace('inf', 0, inplace=True)
            max = use_df.max()
            # print len( max )
            filter_100 = max[max>105]
            if len(filter_100) >0:
                print csv
                print filter_100


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


main()

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)