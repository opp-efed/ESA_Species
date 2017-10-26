import arcpy, traceback, os, sys, csv, time, datetime
from arcpy import env
import functions

inDict = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\ClipDict.csv"


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

with open(inDict, 'rU') as inputFile:

    for line in inputFile:
        line = line.split(',')
        finalGDB = line[0]
        raster = line[1]
        path, inGDB = os.path.split(finalGDB)
        out_working = path + os.sep + 'Working.gdb'
        CreateGDB(path, 'Working.gdb', out_working)
        print finalGDB
        for fc in fcs_in_workspace(finalGDB):
            infc = finalGDB + os.sep +fc
            start_loop = datetime.datetime.now()
            print fc
            Topo = fc.split("_")
            if Topo[0] == 'Topo' and Topo[1] != 'T':
                continue

            listfields = [f.name for f in arcpy.ListFields(infc)]
            if "EntityID" not in listfields:
                print "adding entity id"
                arcpy.AddField_management(infc, "EntityID", "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED", "")
                with arcpy.da.SearchCursor(infc, [ "EntityID"])as cursor:
                    for row in cursor:
                        filename = str(infc)
                        entid = filename.split("_")
                        entid = entid[2]
                        with arcpy.da.UpdateCursor(infc, ["FileName", "EntityID"]) as update:
                            for line in update:
                                line[1] = str(entid)
                                update.updateRow(line)
                            del update, line
                    del cursor, row
            if "FileName" not in listfields:
                print "adding filename"
                arcpy.AddField_management(infc, "FileName", "TEXT", "", "", "40", "", "NULLABLE", "NON_REQUIRED", "")
                with arcpy.da.SearchCursor(infc, ["FileName", "EntityID"])as cursor:
                    for row in cursor:
                        filename = str(infc)

                        with arcpy.da.UpdateCursor(infc, ["FileName", "EntityID"]) as update:
                            for line in update:
                                line[0] = filename
                                update.updateRow(line)
                            del update, line
                    del cursor, row


            else:
                with arcpy.da.SearchCursor(infc, ["EntityID", "SUM"])as cursor:
                    for row in cursor:
                        current_entid = row[0]
                        if row[1] > -1:
                            continue
                        elif row[1] == -1:
                            with arcpy.da.UpdateCursor(infc, ("EntityID", "SUM")) as update:
                                for line in update:
                                    entid = line[0]
                                    if entid == current_entid:
                                        if line[0] == entid:
                                            line[1] = 0
                                            update.updateRow(line)
                                            continue
                                    else:
                                        continue
                            del line, update
                        else:

                            print row[1]
                            if row[1] < -1:

                                entid = row[0]

                                print entid
                                whereclause = "EntityID = '%s'" % (entid)
                                arcpy.Delete_management("fc_lyr")
                                arcpy.MakeFeatureLayer_management(infc, "fc_lyr", whereclause)
                                for row in arcpy.da.SearchCursor("fc_lyr", ["SHAPE@", "EntityID"]):
                                    extent = row[0].extent
                                    Xmin = extent.XMin
                                    Ymin = extent.YMin
                                    Xmax = extent.XMax
                                    Ymax = extent.YMax
                                    extent_layer = str(Xmin) + " " + str(Ymin) + " " + str(Xmax) + " " + str(Ymax)
                                    print extent_layer
                                    outraster = out_working + os.sep + "clip_" + entid + "_" + str(fc)
                                    arcpy.env.workspace = out_working
                                    arcpy.env.overwriteOutput = True
                                    # outraster = out_working+os.sep+str(raster)+ '_' +str(entid)
                                    try:
                                        arcpy.Clip_management(raster, extent_layer, outraster, "fc_lyr", "2147483647",
                                                              "ClippingGeometry", "NO_MAINTAIN_EXTENT")
                                        arcpy.BuildRasterAttributeTable_management(outraster, "Overwrite")
                                        count = int(arcpy.GetCount_management(outraster).getOutput(0))
                                        if count == 1:
                                            with arcpy.da.SearchCursor(outraster, ["Value", "Count"]) as cursor:
                                                for row in cursor:
                                                    if row[0] == 1:
                                                        sum = row[1]
                                                        print row[0]
                                                        print sum

                                                        with arcpy.da.UpdateCursor(infc, ("EntityID", "SUM")) as update:
                                                            for line in update:
                                                                if line[0] == entid:
                                                                    line[1] = sum
                                                                    update.updateRow(line)
                                                                    continue
                                                                else:
                                                                    continue
                                                        del update, line
                                                    else:
                                                        with arcpy.da.UpdateCursor(infc, ("EntityID", "SUM")) as update:
                                                            for line in update:
                                                                if line[0] == entid:
                                                                    line[1] = 0
                                                                    update.updateRow(line)
                                                                    continue
                                                                else:
                                                                    continue
                                                        del update, line
                                            del row, cursor

                                        else:
                                            with arcpy.da.SearchCursor(outraster, ["Value", "Count"]) as cursor:
                                                for row in cursor:
                                                    if row[0] == 1:
                                                        sum = row[1]
                                                        print row[0]
                                                        print sum



                                                        with arcpy.da.UpdateCursor(infc, ("EntityID", "SUM")) as update:
                                                            for line in update:
                                                                if line[0] == entid:
                                                                    line[1] = sum
                                                                    update.updateRow(line)
                                                                    continue
                                                                else:
                                                                    continue
                                                        del update, line
                                                else:
                                                    continue
                                            del row, cursor
                                    except arcpy.ExecuteError, error:
                                        code = str(error)
                                        code = code.split(":")
                                        code = code[0]
                                        print code
                                        if code == "ERROR 001566":
                                            with arcpy.da.UpdateCursor(infc, ("EntityID", "SUM")) as update:
                                                for line in update:
                                                    if line[0] == entid:
                                                        line[1] = 0
                                                        update.updateRow(line)
                                                        continue
                                                    else:
                                                        continue
                                        else:
                                            print error
                   # del cursor, row

        print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)
inputFile.close()
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)