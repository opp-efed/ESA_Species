##  Zonal statistics on a set of overlapping
import arcpy, traceback, os, sys, time
from arcpy import env

env.overwriteOutput = True
parID = "parID"
parID2 = "parID_1"

dem = r'C:\From_MXD\ARC2MDEM_Clip'

outpath_working = ''
outpath_final = ''
layerpath = ''

runID = ''

mxdpath = ''
mxdname = ''

subset = "subset"
count = 0
mxdlocation = mxdpath + os.sep + mxdname

joinerror = -8888
zoneerror =-99999
try:
    def showPyMessage():
        arcpy.AddMessage(str(time.ctime()) + " - " + message)


    def Get_V(aKey):
        try:
            return smallDict[aKey]
        except:
            return (-1)


    def pgonsCount(aLayer):
        result = arcpy.GetCount_management(aLayer)
        return int(result.getOutput(0))


    mxd = arcpy.mapping.MapDocument(mxdlocation)
    layers = arcpy.mapping.ListLayers(mxd)
    minBound, parentLR = layers[0], layers[1]
    with arcpy.da.SearchCursor(minBound, ("SHAPE@", "ID")) as clipper:
        count += 1
        for rcrd in clipper:
            with arcpy.da.SearchCursor(parentLR,("SUM") )as cursor:
                for row in cursor:
                    if row[0] is not None:
                        continue
                del row, cursor

            dbf = outpath_working + os.sep + "stat_" + str(count)
            joinLR = outpath_working + os.sep + 'SD_' + str(count)
            feat = rcrd[0]
            env.extent = feat.extent
            fp = '"ID"=' + str(rcrd[1])
            parentLR.definitionQuery = fp
            nF = pgonsCount(parentLR)
            arcpy.AddMessage("Processing subset %i containing %i polygons" % (rcrd[1], nF))
            arcpy.AddMessage("Defining neighbours...")
            try:
                arcpy.SpatialJoin_analysis(parentLR, parentLR, joinLR, "JOIN_ONE_TO_MANY")
            except:
                print "Failed Join"
                with arcpy.da.UpdateCursor(parentLR, (parID, "ID","SUM")) as cursor:
                    for row in cursor:
                        id = row[1]
                        if id == rcrd[1]:
                            row[1] = joinerror
                            cursor.updateRow(row)

                break

            arcpy.AddMessage("Creating empty dictionary")
            dictFeatures = {}
            with arcpy.da.SearchCursor(parentLR, parID) as cursor:
                for row in cursor:
                    dictFeatures[row[0]] = ()
                del row, cursor
            arcpy.AddMessage("Assigning neighbours...")
            nF = pgonsCount(joinLR)
            arcpy.SetProgressor("step", "", 0, nF, 1)
            with arcpy.da.SearchCursor(joinLR, (parID, parID2)) as cursor:
                for row in cursor:
                    aKey = row[0]
                    aList = dictFeatures[aKey]
                    aList += (row[1],)
                    dictFeatures[aKey] = aList
                    arcpy.SetProgressorPosition()
                del row, cursor
            arcpy.AddMessage("Defining non-overlapping subsets...")
            runNo = 0
            while (True):
                parentLR.definitionQuery = fp
                toShow, toHide = (), ()
                nF = len(dictFeatures)
                arcpy.SetProgressor("step", "", 0, nF, 1)
                for item in dictFeatures:
                    if item not in toShow and item not in toHide:
                        toShow += (item,)
                        toHide += (dictFeatures[item])
                    arcpy.SetProgressorPosition()
                m = len(toShow)
                quer = '"parID" IN ' + str(toShow) + " AND " + fp
                if m == 1:
                    quer = '"parID" = ' + str(toShow[0]) + " AND " + fp
                parentLR.definitionQuery = quer
                runNo += 1
                arcpy.AddMessage("Run %i, %i polygon(s) found" % (runNo, m))
                arcpy.AddMessage("Running Statistics...")
                try:

                    arcpy.gp.ZonalStatisticsAsTable_sa(parentLR, parID, dem, dbf, "DATA", "SUM")
                except:
                    "Failed Zonal"
                    with arcpy.da.UpdateCursor(parentLR, (parID, "ID","SUM")) as cursor:
                        for row in cursor:
                            id = row[1]
                            if id == rcrd[1]:
                                row[1] = zoneerror
                                cursor.updateRow(row)
                        break

                arcpy.AddMessage("Data transfer...")
                smallDict = {}
                with arcpy.da.SearchCursor(dbf, (parID, "SUM")) as cursor:
                    for row in cursor:
                        smallDict[row[0]] = row[1]
                    del row, cursor
                with arcpy.da.UpdateCursor(parentLR, (parID, "SUM")) as cursor:
                    for row in cursor:
                        aKey = row[0]
                        row[1] = Get_V(aKey)
                        cursor.updateRow(row)
                    del row, cursor
                for item in toShow:
                    del dictFeatures[item]
                m = len(dictFeatures)
                if m == 0:
                    break
            parentLR.definitionQuery = fp
            del smallDict, dictFeatures
    parentLR.definitionQuery = ''

except:
    message = "\n*** PYTHON ERRORS *** ";
    showPyMessage()
    message = "Python Traceback Info: " + traceback.format_tb(sys.exc_info()[2])[0];
    showPyMessage()
    message = "Python Error Info: " + str(sys.exc_type) + ": " + str(sys.exc_value) + "\n";
    showPyMessage()

desc = arcpy.Describe(parentLR)
path = desc.path

outFC = outpath_final + os.sep + str(runID)
arcpy.CopyFeatures_management(path, outFC)
