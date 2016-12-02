##  Zonal Histogram
import os
import datetime

import arcpy

import functions

inlocation = r"L:\Workspace\ESA_Species\Step3\10417_Files\SH_Ranges_10147.gdb"

results = r'L:\Workspace\ESA_Species\Step3\10417_Files\SETAC_results\Setac_results_patch7.gdb'
zoneField = "Value"
FullFile= True
scratchpath= r'L:\Workspace\ESA_Species\Step3\10417_Files\SETAC_results\scratch.gdb'
snapRaster = r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015_NAD83"

arcpy.MakeRasterLayer_management(snapRaster,"snap")
arcpy.env.snapRaster = "snap"
pathlist = ['L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb']

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
    arcpy.MakeRasterLayer_management(infc, "fc_lyr")
    if FullFile:
        runID = fc + "_" + raster
        dbf = outpath_final + os.sep + str(runID)
        print dbf
        arcpy.MakeRasterLayer_management(dem, "rdlayer")
        symbologyLayer = "L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_Carbaryl_UseFootprint_160824_euc.lyr"
        arcpy.ApplySymbologyFromLayer_management("rdlayer", symbologyLayer)
        functions.ZonalHist("fc_lyr", zoneField, "rdlayer", dbf, outpath_final, scratchpath)
        print "Full region completed in: {0}".format(datetime.datetime.now() - start_loop)


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

if inlocation[-3:] == 'gdb':
    for raster_in in functions.rasters_in_workspace(inlocation):
        infc = inlocation + os.sep + raster_in
        outpath_final = results
        for v in pathlist:
            rasterLocation = v
            for raster in rasters_in_workspace(rasterLocation):
                print raster
                ZonalHist(rasterLocation, raster, outpath_final, infc,FullFile)
else:
    infc = inlocation
    outpath_final = results
    for v in pathlist:
        rasterLocation = v
        for raster in rasters_in_workspace(rasterLocation):
            ZonalHist(rasterLocation, raster, outpath_final, infc, FullFile)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
