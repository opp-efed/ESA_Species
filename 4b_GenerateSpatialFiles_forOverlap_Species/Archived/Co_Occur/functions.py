import arcpy, traceback, os, sys, csv, time, datetime
from arcpy import env


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster


def pgonsCount(aLayer):
    result = arcpy.GetCount_management(aLayer)
    return int(result.getOutput(0))


def Get_V(aKey, smallDict):
    try:
        return smallDict[aKey]
    except:
        return (-1)


def createdicts(csvfile, key):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname


def SpatialJoin(joinLR, parentLR, rcrd, errorjoin):
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.overwriteOutput = True
    try:
        arcpy.SpatialJoin_analysis(parentLR, parentLR, joinLR, "JOIN_ONE_TO_MANY")
    except:
        print "Failed Join: " + str(rcrd[1])
        with arcpy.da.UpdateCursor(parentLR, ("SUM")) as cursor:
            for row in cursor:
                row[0] = errorjoin
                cursor.updateRow(row)
                continue


def ZonalStats(outpath, parentLR, parID, dem, dbf, errorzonal):
    arcpy.CheckOutExtension("Spatial")
    try:
        arcpy.env.workspace = outpath
        arcpy.env.overwriteOutput = True
        arcpy.AddMessage("Running Statistics...")
        arcpy.gp.ZonalStatisticsAsTable_sa(parentLR, parID, dem, dbf, "DATA", "SUM")
        code = 100
        return code

    except:
        with arcpy.da.UpdateCursor(parentLR, ("SUM")) as cursor:
            for row in cursor:
                row[0] = errorzonal
                cursor.updateRow(row)
            code = errorzonal
            return code


##  Zonal statistics on a set of overlapping

def CoOccur(outpath, runID, mb, infc, lyrPath, raster, dem):
    errorjoin = int(-88888)
    errorzonal = int(-99999)
    errorcode = int(-66666)
    parID = "parID"
    parID2 = "parID_1"
    arcpy.CheckOutExtension("Spatial")

    subset = "subset"
    print "Run to " + str(runID)

    arcpy.env.overwriteOutput = True

    minBound = "minB_lyr"
    parentLR = "parent_lyr"

    arcpy.Delete_management(minBound)
    arcpy.Delete_management(parentLR)
    arcpy.MakeFeatureLayer_management(mb, minBound)
    arcpy.MakeFeatureLayer_management(infc, parentLR)

    with arcpy.da.SearchCursor(minBound, ("SHAPE@", "ID")) as clipper:
        count = 1
        out_layer = lyrPath + os.sep + str(runID) + str(count) + '.lyr'
        arcpy.SaveToLayerFile_management(parentLR, out_layer, "ABSOLUTE")
        # noinspection PyUnboundLocalVariable
        for rcrd in clipper:
            feat = rcrd[0]
            # print feat
            env.extent = feat.extent
            # z =rcrd[1]
            # print z
            fp = '"ID"=' + str(rcrd[1])
            print fp
            parentLR.definitionQuery = fp
            nF = pgonsCount(parentLR)
            if nF is 0:
                with arcpy.da.UpdateCursor(parentLR, ("SUM")) as cursor:
                    for row in cursor:
                        row[0] = errorcode
                        cursor.updateRow(row)
                continue
            with arcpy.da.SearchCursor(parentLR, ("ID", "SUM"))as cursor:
                add = 0
                for row in cursor:
                    z = row[1]
                    print z
                    if z is None:
                        add = None
                        break
                    else:
                        add = add + z
                if add is not None:
                    continue
            arcpy.AddMessage("Processing subset %i containing %i polygons" % (rcrd[1], nF))
            arcpy.AddMessage("Defining neighbours...")
            joinLR = outpath + os.sep + "joinLR" + str(runID) + "_" + str(count)
            SpatialJoin(joinLR, parentLR, rcrd, errorjoin)

            count += 1
            arcpy.AddMessage("Creating empty dictionary")
            dictFeatures = {}
            with arcpy.da.SearchCursor(parentLR, parID)as cursor:
                for row in cursor:
                    dictFeatures[row[0]] = ()
            # del row, cursor
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
            # del row, cursor
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
                # outdbf = outpath+os.sep+str(dbf)+"_"+str(count)
                dbf = outpath + os.sep + "stat_" + str(runID) + "_" + str(raster) + str(count)
                codezone = ZonalStats(outpath, parentLR, parID, dem, dbf, errorzonal)
                count += 1
                if codezone == errorzonal:
                    break

                else:
                    arcpy.AddMessage("Data transfer...")
                    smallDict = {}
                    fieldnames = [f.name for f in arcpy.ListFields(dbf)]
                    with arcpy.da.SearchCursor(dbf, (parID, "SUM")) as cursor:
                        for row in cursor:
                            smallDict[row[0]] = row[1]
                    # del row, cursor
                    with arcpy.da.UpdateCursor(parentLR, (parID, "SUM")) as cursor:
                        for row in cursor:
                            aKey = row[0]
                            row[1] = Get_V(aKey, smallDict)
                            cursor.updateRow(row)
                    # del row, cursor
                    for item in toShow:
                        del dictFeatures[item]
                    m = len(dictFeatures)
                    if m == 0:
                        break

            parentLR.definitionQuery = fp
            # del smallDict, dictFeatures
    parentLR.definitionQuery = ''
