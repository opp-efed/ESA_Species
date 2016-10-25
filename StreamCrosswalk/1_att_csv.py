import arcpy
import datetime
import os
import pandas as pd

inFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_results'
outFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_csv'


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


def export_dbf_to_csv(tbl_list, in_location, out_location):
    for table in tbl_list:
        start_table = datetime.datetime.now()
        intable = in_location + os.sep + table
        out_table = table + '.csv'
        final_csv = out_location + os.sep + out_table

        list_fields = [f.name for f in arcpy.ListFields(intable)]
        att_array = arcpy.da.TableToNumPyArray(intable, list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df.to_csv(final_csv)
        end = datetime.datetime.now()
        elapsed_table = end - start_table
        print 'Completed Export of {0} to {1} in {2}'.format(out_table, out_folder, elapsed_table)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if inFolder[-3:] != 'gdb':
    list_folder = os.listdir(inFolder)

    for folder in list_folder:
        start_folder = datetime.datetime.now()
        print '\n{0}'.format(folder)
        out_folder = outFolder + os.sep + folder
        createdirectory(out_folder)
        current_path = inFolder + os.sep + folder
        gdb_list = os.listdir(current_path)
        gdb_list = [gdb for gdb in gdb_list if gdb.endswith('gdb')]
        completed_tables = os.listdir(out_folder)
        completed_tables = [csv.replace('.csv', '') for csv in completed_tables if csv.endswith('csv')]

        for gdb in gdb_list:
            in_folder = current_path + os.sep + gdb
            arcpy.env.workspace = in_folder
            table_list = arcpy.ListTables()
            table_remove = [table for table in table_list if table in completed_tables]
            table_list = [table for table in table_list if table not in completed_tables]
            if len(table_list) == 0:
                print 'Already completed all exports for {0}'.format(table_remove)
            else:
                export_dbf_to_csv(table_list, in_folder, out_folder)

        end = datetime.datetime.now()
        print "End Time: " + end.ctime()
        elapsed = end - start_folder
        print "Elapsed  Time: " + str(elapsed)

else:
    out_folder = outFolder
    completed_tables = os.listdir(out_folder)
    completed_tables = [csv.replace('.csv', '') for csv in completed_tables if csv.endswith('csv')]
    arcpy.env.workspace = inFolder
    table_list = arcpy.ListTables()
    table_list = [table for table in table_list if table not in completed_tables]
    table_remove = [table for table in table_list if table in completed_tables]
    table_list = [table for table in table_list if table not in completed_tables]
    if len(table_list) == 0:
            print 'Already completed all exports for {0}'.format(table_remove)
    else:
        export_dbf_to_csv(table_list, inFolder, out_folder)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
