##  Zonal statistics on a set of overlapping
import arcpy, traceback, os, sys, csv, time, datetime
from arcpy import env

parID = "parID"
parID2 = "parID_1"

outpath_final = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\Amphibians\R_AmphibiansL48only.gdb'
outpath = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\Amphibians\Working.gdb"
lyrPath = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\Amphibians\lyr"

flag ="AmpL48only"
mapname ="UseMasterAmpL48_10.1.mxd"

rasterLocation = r"C:\Users\Admin\Documents\Jen\Workspace\UseSites\CONUS_Ag_151103.gdb"


exportdict = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\NewLayersDict_export.csv"
mappath = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\MxdL48only"

errorjoin = int(-88888)
errorzonal = int(-99999)
errorcode = int(-66666)
othercode = int(-77777)
arcpy.CheckOutExtension("Spatial")


def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster


def createdicts(csvfile, key):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname


start_script = datetime.datetime.now()


export_dict = {}


export_dict = createdicts(exportdict, export_dict)
for k in export_dict:
    value = export_dict[k]

for raster in rasters_in_workspace(rasterLocation):
    count =0
    runID = str(flag) + '_' + str(raster)
    #print runID
    dem = rasterLocation + os.sep + str(raster)
    #print dem
    dem =dem.replace('\\\\','\\')
    #print export_dict
    out=export_dict[str(dem)]

    outFC_raster = out + os.sep + runID
    #print outFC_raster

    if arcpy.Exists(outFC_raster):
        print "Already complete analysis for {0}".format(raster)
        continue


    joinLR = outpath + os.sep + str(runID)
    subset = "subset"
    print "Run to " + str(runID)

    try:
        def showPyMessage():
            arcpy.AddMessage(str(time.ctime()) + " - " + message)


        def pgonsCount(aLayer):
            result = arcpy.GetCount_management(aLayer)
            return int(result.getOutput(0))


        def Get_V(aKey):
            try:
                return smallDict[aKey]
            except:
                return (-1)


        # startb = time.time()
        # start_times(startb)
        arcpy.env.overwriteOutput=True
        start_loop = datetime.datetime.now()
        maplocation = mappath + os.sep + mapname
        mxd = arcpy.mapping.MapDocument(maplocation)
        layers = arcpy.mapping.ListLayers(mxd)
        minBound, parentLR = layers[0], layers[1]
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
                try:
                    joinLR = outpath + os.sep + "joinLR" + str(runID) + "_" + str(count)
                    arcpy.SpatialJoin_analysis(parentLR, parentLR, joinLR, "JOIN_ONE_TO_MANY")
                except:
                    print "Failed Join: " + str(rcrd[1])
                    with arcpy.da.UpdateCursor(parentLR, ("SUM")) as cursor:
                        for row in cursor:
                            row[0] = errorjoin
                            cursor.updateRow(row)

                    continue
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
                    try:
                        arcpy.env.workspace = outpath
                        dbf = outpath + os.sep + "stat_" + str(runID) + "_" + str(raster) + str(count)
                        arcpy.AddMessage("Running Statistics...")
                        arcpy.gp.ZonalStatisticsAsTable_sa(parentLR, parID, dem, dbf, "DATA", "SUM")
                        count +=1
                    except:
                        with arcpy.da.UpdateCursor(parentLR, ("SUM")) as cursor:
                            for row in cursor:
                                row[0] = errorzonal
                                cursor.updateRow(row)
                        message = "\n*** PYTHON ERRORS *** ";
                        showPyMessage()
                        message = "Python Traceback Info: " + traceback.format_tb(sys.exc_info()[2])[0];
                        showPyMessage()
                        message = "Python Error Info: " + str(sys.exc_type) + ": " + str(sys.exc_value) + "\n";
                        showPyMessage()
                        break
                        # arcpy.gp.ZonalStatisticsAsTable_sa(out_layer, parID, dem, dbf, "DATA", "SUM")
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
                            row[1] = Get_V(aKey)
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
    except:
        message = "\n*** PYTHON ERRORS *** ";
        showPyMessage()
        message = "Python Traceback Info: " + traceback.format_tb(sys.exc_info()[2])[0];
        showPyMessage()
        message = "Python Error Info: " + str(sys.exc_type) + ": " + str(sys.exc_value) + "\n";
        showPyMessage()
        continue

    print "Run to " + str(runID)
    try:
        outFC = outpath_final + os.sep + runID
        desc = arcpy.Describe(parentLR)
        filepath = desc.catalogPath
        # print filepath

        print outFC
        if not arcpy.Exists(outFC):
            arcpy.Copy_management(filepath, outFC)
            print "Exported: " + str(outFC)

        print outFC_raster
        if not arcpy.Exists(outFC_raster):
            arcpy.Copy_management(filepath, outFC_raster)
            print "Exported: " + str(outFC_raster)


        with arcpy.da.UpdateCursor(parentLR, ("SUM")) as cursor:
            for row in cursor:
                if row[0] > -1:
                    row[0] = None
                    cursor.updateRow(row)
    except:
        "Failed update SUM to none"

    del raster
    print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
