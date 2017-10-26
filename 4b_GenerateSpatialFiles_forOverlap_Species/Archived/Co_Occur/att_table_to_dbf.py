import arcpy
from simpledbf import Dbf5
import os

MultiFolder = True
inFolder ='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\ResultsByUse'
ws = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur"
name_dir = "ResultsByUse_Tables"

def fcs_in_workspace(workspace):
  arcpy.env.workspace = workspace
  for fc in arcpy.ListFeatureClasses():
    yield(fc)
  for ws in arcpy.ListWorkspaces():
    for fc in fcs_in_workspace(ws):
        yield fc

def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)

FolderList = os.listdir(inFolder)
print FolderList
File_dir = ws + os.sep + str(name_dir)
DBF_dir = File_dir + os.sep + 'DBF'
CSV_dir = File_dir + os.sep + 'CSV'
createdirectory(DBF_dir)
createdirectory(CSV_dir)
counter = 0

for value in FolderList:
    FinalGDB = inFolder+ os.sep + value + os.sep + 'Final.gdb'
    #print FinalGDB
    outfolder = DBF_dir + os.sep + value
    createdirectory(outfolder)
    for fc in fcs_in_workspace(FinalGDB):
        print fc
        counter +=1
        table = "{0}.dbf".format(fc)
        outtable = outfolder + os.sep + table
        tableview = "tbl_view_" + str(counter)
        arcpy.Delete_management("fc_lyr")
        if arcpy.Exists(outtable):
            continue
        else:
            arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
            arcpy.MakeTableView_management("fc_lyr", tableview)
            arcpy.TableToTable_conversion(tableview, outfolder, table)


    arcpy.env.workspace = outfolder
    files = arcpy.ListTables()
    outcsv = CSV_dir + os.sep + value
    createdirectory(outcsv)
    for f in files:
        if f[-3:].lower() == 'dbf':
            stripdbf = f.strip('.dbf')
            outtable = outcsv + os.sep + stripdbf +'.csv'
            print outtable
            intable = outfolder + os.sep + f
            dbf= Dbf5(intable)
            dbf.to_csv(outtable)