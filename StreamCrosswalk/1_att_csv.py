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
        for gdb in gdb_list:
            arcpy.env.workspace = current_path + os.sep + gdb
            table_list = arcpy.ListTables()
            for table in table_list:
                list_field = [f.name for f in arcpy.ListFields(table)]
                start_table = datetime.datetime.now()
                intable = current_path + os.sep + gdb + os.sep + table
                out_table = table + '.csv'
                final_csv = out_folder + os.sep + out_table
                if not os.path.exists(final_csv):
                    list_fields = [f.name for f in arcpy.ListFields(intable)]
                    att_array = arcpy.da.TableToNumPyArray(intable, list_fields)
                    att_df = pd.DataFrame(data=att_array)
                    att_df.to_csv(final_csv)
                    end = datetime.datetime.now()
                    elapsed_table = end - start_table
                    print 'Completed Export of {0} to {1} in {2}'.format(out_table, out_folder, elapsed_table)
                    del att_df
                else:
                    print 'Already export table {0}'.format(final_csv)
        end = datetime.datetime.now()
        print "End Time: " + end.ctime()
        elapsed = end - start_folder
        print "Elapsed  Time: " + str(elapsed)

else:
    out_folder = outFolder
    arcpy.env.workspace = inFolder
    table_list = arcpy.ListTables()
    for table in table_list:
        start_table = datetime.datetime.now()
        intable = inFolder + os.sep + table
        out_table = table + '.csv'
        final_csv = out_folder + os.sep + out_table
        if not os.path.exists(final_csv):
            list_fields = [f.name for f in arcpy.ListFields(intable)]
            att_array = arcpy.da.TableToNumPyArray(intable, list_fields)
            att_df = pd.DataFrame(data=att_array)
            att_df.to_csv(final_csv)
            end = datetime.datetime.now()
            elapsed_table = end - start_table
            print 'Completed Export of {0} to {1} in {2}'.format(out_table, out_folder, elapsed_table)
        else:
            print 'Already export table {0}'.format(final_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
