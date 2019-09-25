import pandas as pd
import os
import datetime


in_results = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\Enlist Duo\Request_20190906\Corn_10_Cotton_20_Soybean_40_Overlap'
# directory for look-up tables - key_col is the header used as the zone in the specie runs and will be joined to the VALUE field

# Can be the lookup for CH or Range
in_look_up = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\Enlist Duo\Request_20190906\LookUps"
key_col = 'HUCID'  # InterID, 'HUCID'
out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\Enlist Duo\Request_20190906\Tabulated_Overlap'


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if not os.path.exists(out_location):
    os.mkdir(out_location)

list_results_directory = os.listdir(in_results)
list_lookup = os.listdir(in_look_up)

for folder in list_results_directory:
    out_path = out_location + os.sep + folder
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    csv_list = os.listdir(in_results + os.sep+ folder)
    for csv in csv_list:
        print csv
        if not os.path.exists(out_path + os.sep + csv):
            results_df = pd.read_csv(in_results + os.sep+ folder + os.sep +csv)
            lookup_csv = [t for t in list_lookup if t.startswith(csv.split("_")[0].upper() +"_" + csv.split("_")[1].capitalize())]

            lookup_df = pd.read_csv(in_look_up + os.sep + lookup_csv[0])
            # merged_df = pd.merge(results_df,lookup_df, how='outer', left_on='VALUE', right_on= key_col)
            merged_df = pd.merge(results_df,lookup_df, how='left', left_on='VALUE', right_on= key_col)
            [merged_df.drop(col, axis=1, inplace=True) for col in merged_df.columns.values.tolist() if
             col.startswith('Un')]
            merged_df.to_csv(out_path + os.sep + csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)