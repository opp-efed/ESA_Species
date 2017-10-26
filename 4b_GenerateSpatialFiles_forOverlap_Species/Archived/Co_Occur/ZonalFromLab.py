##  Zonal statistics on a set of overlapping
import arcpy, traceback, os, sys, time, datetime, csv
from arcpy import env
env.overwriteOutput = True

parID="parID"
parID2="parID_1"



outpath = "C:\\Users\\Admin\\Documents\\Jen\\5_20_15\\Working_R_FWS_20150520.gdb"
lyrPath = "C:\\Users\\Admin\\Documents\\Jen\\5_20_15"

mappath = "C:\\Users\\Admin\\Documents\\Jen\\5_20_15\\MXDs\\UseLayers"

rasterLocation =r"C:\Users\Admin\Documents\Jen\NewClor_layers.gdb"

rasterdict = "C:\\Users\\Admin\\Documents\\Jen\\R_NMFS_Dicts\\NewLayersDict.csv"
exportdict = "C:\\Users\\Admin\\Documents\\Jen\\R_NMFS_Dicts\\NewLayersDict_export.csv"

errorjoin= int(-88888)
errorzonal = int(-99999)
errorcode = int(-66666)
othercode= int (-77777)
arcpy.CheckOutExtension("Spatial")



def raster_in_workspace(workspace):
  arcpy.env.workspace = workspace
  for raster in arcpy.ListRasters():
    yield(raster)
  for ws in arcpy.ListWorkspaces():
    for raster in raster_in_workspace(ws):
        yield raster

def createdicts(csvfile, key):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname

env.workspace = outpath

#start = time.time()
#start_times(start)
start_script = datetime.datetime.now()
mapname_dict = {}
mapname_dict = createdicts(rasterdict, mapname_dict)
export_dict = {}

export_dict = createdicts(exportdict, export_dict)
for k in mapname_dict:
    value = mapname_dict[k]
    print value


for raster in raster_in_workspace(rasterLocation):
    print str(raster)
    dem = rasterLocation +os.sep+ str(raster)
    print dem
    mapname = mapname_dict[dem]
    print mapname
    runID = mapname[:-4]
    print runID
    #arcpy.BuildRasterAttributeTable_management(dem, 'None')
    dbf="stat_" + str(runID)
    joinLR=outpath +os.sep + str(runID)
    subset="subset"
    print "Run to " + str(runID)+ "5/20"


    try:
        def showPyMessage():
            arcpy.AddMessage(str(time.ctime()) + " - " + message)

        def pgonsCount(aLayer):
            result=arcpy.GetCount_management(aLayer)
            return int(result.getOutput(0))
        def Get_V(aKey):
            try:
                return smallDict[aKey]
            except:
                return (-1)
        #startb = time.time()
        #start_times(startb)
        start_loop = datetime.datetime.now()
        maplocation = mappath +os.sep + mapname
        mxd = arcpy.mapping.MapDocument(maplocation)
        layers = arcpy.mapping.ListLayers(mxd)
        minBound,parentLR=layers[0],layers[1]
        with arcpy.da.SearchCursor(minBound, ("SHAPE@","ID")) as clipper:
            count =1
            out_layer =lyrPath+os.sep+ str(runID)+str(count)+'.lyr'
            arcpy.SaveToLayerFile_management(parentLR, out_layer, "ABSOLUTE")
            # noinspection PyUnboundLocalVariable
            for rcrd in clipper:
                feat=rcrd[0]
                #print feat
                env.extent=feat.extent
            #z =rcrd[1]
            #print z
                fp='"ID"='+str(rcrd[1])
                print fp
                parentLR.definitionQuery=fp
                nF=pgonsCount(parentLR)
                if nF is 0:
                    with arcpy.da.UpdateCursor(parentLR,("SUM")) as cursor:
                        for row in cursor:
                            row[0] = errorcode
                            cursor.updateRow(row)
                    continue
                with arcpy.da.SearchCursor(parentLR,("ID","SUM"))as cursor:
                    add=0
                    for row in cursor:
                        z= row[1]
                        print z
                        if z is None:
                            add = None
                            break
                        else:
                            add= add+z
                    if add is not None:
                        continue
                arcpy.AddMessage("Processing subset %i containing %i polygons" %(rcrd[1],nF))
                arcpy.AddMessage("Defining neighbours...")
                try:
                    joinLR=outpath +os.sep + "joinLR"+str(runID)+"_"+str(count)
                    arcpy.SpatialJoin_analysis(parentLR, parentLR, joinLR,"JOIN_ONE_TO_MANY")
                except:
                    print "Failed Join: " + str(rcrd[1])
                    with arcpy.da.UpdateCursor(parentLR,("SUM")) as cursor:
                        for row in cursor:
                            row[0] = errorjoin
                            cursor.updateRow(row)
                    continue

                count += 1
                arcpy.AddMessage("Creating empty dictionary")
                dictFeatures = {}
                with arcpy.da.SearchCursor(parentLR, parID)as cursor:
                    for row in cursor:
                        dictFeatures[row[0]]=()
                #del row, cursor
                arcpy.AddMessage("Assigning neighbours...")
                arcpy.MakeFeatureLayer_management(joinLR,"join")
                nF=pgonsCount("join")
                arcpy.SetProgressor("step", "", 0, nF,1)
                with arcpy.da.SearchCursor("join", (parID,parID2)) as cursor:
                    for row in cursor:
                        aKey=row[0]
                        aList=dictFeatures[aKey]
                        aList+=(row[1],)
                        dictFeatures[aKey]=aList
                        arcpy.SetProgressorPosition()
                #del row, cursor
                arcpy.AddMessage("Defining non-overlapping subsets...")
                runNo=0
                while (True):
                    parentLR.definitionQuery=fp
                    toShow,toHide=(),()
                    nF=len(dictFeatures)
                    arcpy.SetProgressor("step", "", 0, nF,1)
                    for item in dictFeatures:
                        if item not in toShow and item not in toHide:
                            toShow+=(item,)
                            toHide+=(dictFeatures[item])
                        arcpy.SetProgressorPosition()
                    m=len(toShow)
                    quer='"parID" IN '+str(toShow)+ " AND "+fp
                    if m==1:
                        quer='"parID" = '+str(toShow[0])+ " AND "+fp
                    parentLR.definitionQuery=quer
                    runNo+=1
                    arcpy.AddMessage("Run %i, %i polygon(s) found" % (runNo,m))
                #outdbf = outpath+os.sep+str(dbf)+"_"+str(count)
                    try:
                        arcpy.env.workspace = outpath
                        dbf=outpath + os.sep + "stat_" + str(runID)+"_"+str(raster)+str(count)
                        arcpy.AddMessage("Running Statistics...")
                        arcpy.gp.ZonalStatisticsAsTable_sa(parentLR, parID, dem, dbf, "DATA", "SUM")
                    except:
                        with arcpy.da.UpdateCursor(parentLR,("SUM")) as cursor:
                            for row in cursor:
                                row[0] = errorzonal
                                cursor.updateRow(row)
                        message = "\n*** PYTHON ERRORS *** "; showPyMessage()
                        message = "Python Traceback Info: " + traceback.format_tb(sys.exc_info()[2])[0]; showPyMessage()
                        message = "Python Error Info: " +  str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"; showPyMessage()
                        break
                #arcpy.gp.ZonalStatisticsAsTable_sa(out_layer, parID, dem, dbf, "DATA", "SUM")
                    arcpy.AddMessage("Data transfer...")
                    smallDict={}
                    fieldnames = [f.name for f in arcpy.ListFields(dbf)]
                    with arcpy.da.SearchCursor(dbf, (parID,"SUM")) as cursor:
                        for row in cursor:
                            smallDict[row[0]]=row[1]
                    #del row, cursor
                    with arcpy.da.UpdateCursor(parentLR, (parID,"SUM")) as cursor:
                        for row in cursor:
                            aKey=row[0]
                            row[1]=Get_V(aKey)
                            cursor.updateRow(row)
                    #del row, cursor
                    for item in toShow:
                        del dictFeatures[item]
                    m=len(dictFeatures)
                    if m==0:
                        break

                parentLR.definitionQuery=fp
            #del smallDict, dictFeatures
        parentLR.definitionQuery=''
    except:
        message = "\n*** PYTHON ERRORS *** "; showPyMessage()
        message = "Python Traceback Info: " + traceback.format_tb(sys.exc_info()[2])[0]; showPyMessage()
        message = "Python Error Info: " + str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"; showPyMessage()
        continue

    print "Run to " + str(runID)
    #try:
        #exportlocation = export_dict[dem]
        #outFC = exportlocation+os.sep+str(parentLR)
        #desc = arcpy.Describe(parentLR)
        #filepath= desc.catalogPath
        #print filepath
        #if not arcpy.Exists(outFC):
            #arcpy.Copy_management(filepath,outFC)
            #print "Exported: " + str(parentLR)
    #except:
        #exportlocation = export_dict[dem]
        #print exportlocation
        #outFC = exportlocation+os.sep+str(parentLR)
        #print "Failed Export: " + str(outFC)
    print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)


print "Script completed in: {0}".format(datetime.datetime.now() - start_script)