import arcpy
from simpledbf import Dbf5
import os


inFolder = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\ZonalHis_GAP_Insects.gdb'
outdbf = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Insects_dbf'
outfolder ='J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Insect_csv'

arcpy.env.workspace= inFolder
listtable = arcpy.ListTables()
counter = 0

print "start"
arcpy.env.workspace = inFolder
listtable = arcpy.ListTables()
for table in listtable:
    try:
        outtable = outdbf+ os.sep + table+'.dbf'
        if not arcpy.Exists(outtable):
            print table
            counter += 1
            tableview = "tbl_view_" + str(counter)
            outtable = outdbf+ os.sep + table+'.dbf'
            print outtable
            if not arcpy.Exists(outtable):
                arcpy.MakeTableView_management(table, tableview)
                arcpy.TableToTable_conversion(tableview, outdbf, table)
            else:
                continue
    except:
        continue
arcpy.env.workspace = outdbf
files = arcpy.ListTables()
outcsv = outfolder
for f in files:
    if f[-3:].lower() == 'dbf':
        stripdbf = f.replace('.dbf','.csv')
        outtable = outcsv+ os.sep + stripdbf
        print outtable
        if os.path.exists(outtable):
            continue
        intable = outdbf + os.sep + f
        dbf = Dbf5(intable)
        dbf.to_csv(outtable)

list_csv = os.listdir(outfolder)
print list_csv



