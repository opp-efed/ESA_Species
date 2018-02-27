import os

import arcpy
from simpledbf import Dbf5

import functions

MultiFolder = False
fc= False
inFolder = 'J:\PUR\Counties_145\Counties_145.gdb'
ws = "J:\PUR\Counties_145"
name_dir = "CountiesATT_145"





def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)

File_dir = ws + os.sep + str(name_dir)
DBF_dir = File_dir + os.sep + 'DBF'
CSV_dir = File_dir + os.sep + 'CSV'
createdirectory(File_dir)
createdirectory(DBF_dir)
createdirectory(CSV_dir)

counter = 0


if MultiFolder:
    FolderList = os.listdir(inFolder)
    print FolderList

    for value in FolderList:
        FinalGDB = inFolder + os.sep + value + os.sep + 'Final.gdb'
        # print FinalGDB
        outfolder = DBF_dir + os.sep + value
        createdirectory(outfolder)
        if fc:
            for fc in functions.fcs_in_workspace(FinalGDB):
                print fc
                counter += 1
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

        else:
            for raster in functions.rasters_in_workspace(FinalGDB):
                print raster
                counter += 1
                table = "{0}.dbf".format(fc)
                outtable = outfolder + os.sep + table
                tableview = "tbl_view_" + str(counter)
                arcpy.Delete_management("fc_lyr")
                if arcpy.Exists(outtable):
                    continue
                else:
                    arcpy.MakeRasterLayer_management(raster, "raster_lyr")
                    arcpy.MakeTableView_management("raster_lyr", tableview)
                    arcpy.TableToTable_conversion(tableview, outfolder, table)

        arcpy.env.workspace = outfolder
        files = arcpy.ListTables()
        outcsv = CSV_dir
        createdirectory(outcsv)
        for f in files:
            if f[-3:].lower() == 'dbf':
                stripdbf = f.strip('.dbf')
                outtable = outcsv + os.sep + stripdbf + '.csv'
                print outtable
                if os.path.exists(outtable):
                    continue
                intable = outfolder + os.sep + f
                dbf = Dbf5(intable)
                dbf.to_csv(outtable)
else:
    if fc:
        for fc in functions.fcs_in_workspace(inFolder):
            print fc
            outfolder= DBF_dir
            counter += 1
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

    else:
        for raster in functions.rasters_in_workspace(inFolder):
            print raster
            outfolder= DBF_dir
            counter += 1
            table = "{0}.dbf".format(raster)
            outtable = outfolder + os.sep + table
            tableview = "tbl_view_" + str(counter)
            arcpy.Delete_management("fc_lyr")
            if arcpy.Exists(outtable):
                continue
            else:
                arcpy.MakeRasterLayer_management(raster, "raster_lyr")
                arcpy.MakeTableView_management("raster_lyr", tableview)
                arcpy.TableToTable_conversion(tableview, outfolder, table)

        arcpy.env.workspace = outfolder
        files = arcpy.ListTables()
        outcsv = CSV_dir
        createdirectory(outcsv)
        for f in files:
            if f[-3:].lower() == 'dbf':
                stripdbf = f.replace('.dbf', '.csv')
                outtable = outcsv + os.sep + stripdbf
                if os.path.exists(outtable):
                    continue
                print outtable
                intable = outfolder + os.sep + f
                dbf = Dbf5(intable)
                dbf.to_csv(outtable)
