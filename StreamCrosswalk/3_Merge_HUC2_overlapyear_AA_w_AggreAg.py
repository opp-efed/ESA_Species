import pandas as pd
import os
import datetime

#TODO SET UP LOOP TO DEAL AITH AA AJND AGGREAGEATE
in_folder_ag = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\YearlyCDL\HUC12_transposed'
in_folder_non_ag = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\NonAg\HUC12_transposed'


outFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\FinalTables\Merged\AA and Aggregates'


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

print 'Copy Transposed_AllHUC2_AA.csv Transposed_AllHUC2_aggregated_layers.csv to the merge ag folder'
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)