import arcpy, os, datetime
from arcpy import env
from arcpy.sa import *
import functions

out_working = 'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR'
# out_working="J:\Workspace\Step3_Proposal\Yearly_CDL\spe_orchard.gdb"
inraster = 'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR\YearlyCDL_clippedcounties.gdb'
workingGDB ="C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR\working.gdb"



arcpy.CheckOutExtension("Spatial")

numberCells = 26
rastername = '_buf' + str(numberCells)
skip = []
# 11 ground 26 aeril
zoneValuesDict = dict(spe_vegGround=[26, 56, 60, 61, 68], spe_orchard=[70], spe_corn=[10, 14, 15, 18],
                      spe_cotton=[20, 25, 26, 42], spe_rice=[30], spe_soybean=[40, 42, 45, 48, 14],
                      spe_wheat=[50, 56, 58, 15, 25, 45], spe_othergrain=[80], spe_otherrow=[90], spe_othercrop=[100],
                      spe_pasture=[110])


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


listuse = zoneValuesDict.keys()
print listuse

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

for raster in functions.rasters_in_workspace(inraster):
    print raster
    inloopraster = inraster + os.sep + raster
    start_loop = datetime.datetime.now()
    print "Loop started for raster {1} with buffer {2} at: {0}".format(start_loop, raster, numberCells)
    print numberCells
    for v in listuse:
        if v in skip:
            continue
        else:
            print v
            outGDB = v + '.gdb'
            outpath = out_working + os.sep + outGDB
            CreateGDB(out_working, outGDB, outpath)
            zoneValues = zoneValuesDict[v]
            print zoneValues
            outraster = outpath + os.sep + raster + rastername

            arcpy.env.workspace = workingGDB
            if not arcpy.Exists(outraster):
                outExpand = Expand(inloopraster, numberCells, zoneValues)
                outExpand.save(outraster)
            else:
                continue

    print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
