import arcpy
import os
from simpledbf import Dbf5

DBFfolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\CriticalHabitat\AllSp_MasterList_20161124\DBF'
CSV_dir = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\CriticalHabitat\AllSp_MasterList_20161124\CSV'

arcpy.env.workspace = DBFfolder
files = arcpy.ListTables()
outcsv = CSV_dir

for f in files:
    if f[-3:].lower() == 'dbf':
        stripdbf = f.replace('.dbf', '.csv')
        outtable = CSV_dir + os.sep + stripdbf
        ent = f.split("_")
        if ent[0]== 'Range':
            ent= ent[2]
            print ent

        if os.path.exists(outtable):
            continue
        intable = DBFfolder + os.sep + f
        dbf = Dbf5(intable)
        dbf.to_csv(outtable)
        print "Exported {0}".format(outtable)
