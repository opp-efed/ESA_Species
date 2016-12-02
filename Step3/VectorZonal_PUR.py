# #  Zonal statistics on a set of overlapping
import arcpy, traceback, os, sys, csv, time, datetime
from arcpy import env
import functions

parID = "parID"
parID2 = "parID_1"
ClippedUses = False
zoneField = 'Value'

inGDB = "C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR\CountyRange\CountiesPUR.gdb"

results = 'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR\CountyRange'
zoneField = "GEOID"

if ClippedUses:
    pathlist = ['C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_vegGround.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_orchard.gdb']
else:
    pathlist = ['C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR\CountyRange\GapCounties\ManualGAPClip.gdb']


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


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

for fc in functions.fcs_in_workspace(inGDB):
    infc = inGDB + os.sep + fc

    outgdb = (str(fc) + '.gdb')
    results_FC = results + os.sep + fc
    outpath_final = results_FC + os.sep + outgdb

    createdirectory(results_FC)
    CreateGDB(results_FC, outgdb, outpath_final)

    for v in pathlist:
        rasterLocation = v
        for raster in rasters_in_workspace(rasterLocation):
            raster_entid = raster.split("_")
            raster_entid = raster_entid[5]
            path, flag = os.path.split(rasterLocation)
            flag = flag.replace('.gdb', '')
            runID = str(flag) + '_' + str(raster)
            #print runID
            dem = rasterLocation + os.sep + str(raster)
            #print dem
            dem = dem.replace('\\\\', '\\')

            #print export_dict
            out = outpath_final
            outFC_raster = out + os.sep + runID

            if arcpy.Exists(outFC_raster):
                print "Already complete analysis for {0}".format(raster)
                continue

            arcpy.env.overwriteOutput = True
            start_loop = datetime.datetime.now()

            arcpy.MakeFeatureLayer_management(infc, "fc_lyr")
            for row in arcpy.da.SearchCursor("fc_lyr", ["EntityID"]):
                entid = row[0]
                if raster_entid != entid:
                    continue
                else:

                    dbf = outpath_final + os.sep + str(runID) + "_" + str(raster)
                    print dbf
                    if arcpy.Exists(dbf):
                        continue
                    else:
                        print dem
                        arcpy.MakeRasterLayer_management(dem, "rdlayer")
                        symbologyLayer = "C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\PUR\CountyRange\CDL_2010_rec_clip_123_buf11.lyr"
                        arcpy.ApplySymbologyFromLayer_management ("rdlayer", symbologyLayer)
                        codezone = functions.ZonalHist(infc, zoneField, "rdlayer", dbf, outpath_final)

                        print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

    print "Script completed in: {0}".format(datetime.datetime.now() - start_script)


