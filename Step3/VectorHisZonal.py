##  Zonal Histogram
import os
import datetime

import arcpy

import functions

inlocation = "J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Working.gdb"

results = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Species_GapClipGAP_Insects.gdb'
zoneField = "OBJECTID"
FullFile= False
scratchpath= r'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Species_GapClipGAP_Insects.gdb'
snapRaster = r"J:\Workspace\Step3_Proposal\GAP\National\natgaplandcov_v2_2_1.img"

arcpy.MakeRasterLayer_management(snapRaster,"snap")
arcpy.env.snapRaster = "snap"
pathlist = ['J:\Workspace\Step3_Proposal\GAP\National']

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


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


def createdirectory(path_dir):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)


def ZonalHist(rasterLocation, raster, outpath_final, infc, FullFile):
    dem = rasterLocation + os.sep + str(raster)
    path, fc = os.path.split(infc)
    out = outpath_final
    arcpy.env.overwriteOutput = True
    start_loop = datetime.datetime.now()
    arcpy.MakeFeatureLayer_management(infc, "fc_lyr")
    if FullFile:
        runID = fc
        dbf = outpath_final + os.sep + str(runID)
        print dbf
        arcpy.MakeRasterLayer_management(dem, "rdlayer")
        symbologyLayer = "J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\GAP.lyr"
        arcpy.ApplySymbologyFromLayer_management("rdlayer", symbologyLayer)
        codezone = functions.ZonalHist("fc_lyr", zoneField, "rdlayer", dbf, outpath_final)
        print "Full region completed in: {0}".format(datetime.datetime.now() - start_loop)
    else:
        for row in arcpy.da.SearchCursor("fc_lyr", [zoneField, "SHAPE@"]):
            entid = row[0]
            #whereclause = "EntityID = '%s'" % (id)
            whereclause= "\"OBJECTID\"="+str(entid) #TODO Make this dyanmic to zon field
            lyr = "lyr"
            arcpy.MakeFeatureLayer_management("fc_lyr", lyr, whereclause)
            runID = str(raster[:-4]) + "_" + str(entid)
            print runID
            extent = row[1].extent
            Xmin = extent.XMin
            Ymin = extent.YMin
            Xmax = extent.XMax
            Ymax = extent.YMax
            extent_layer = str(Xmin) + " " + str(Ymin) + " " + str(Xmax) + " " + str(Ymax)
            dbf = outpath_final + os.sep + str(runID)
            print dbf
            if arcpy.Exists(dbf):
                continue
            else:
                arcpy.env.extent = extent_layer
                print extent_layer
                arcpy.MakeRasterLayer_management(dem, "rdlayer")
                symbologyLayer = "J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\GAP.lyr"
                arcpy.ApplySymbologyFromLayer_management("rdlayer", symbologyLayer)
                functions.ZonalHist(lyr, zoneField, "rdlayer", dbf, outpath_final, scratchpath)
                del lyr

                print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

if inlocation[-3:] == 'gdb':
    for fc in functions.fcs_in_workspace(inlocation):
        infc = inlocation + os.sep + fc
        outpath_final = results
        for v in pathlist:
            rasterLocation = v
            for raster in rasters_in_workspace(rasterLocation):
                ZonalHist(rasterLocation, raster, outpath_final, infc,FullFile)
else:
    infc = inlocation
    outpath_final = results
    for v in pathlist:
        rasterLocation = v
        for raster in rasters_in_workspace(rasterLocation):
            ZonalHist(rasterLocation, raster, outpath_final, infc, FullFile)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
