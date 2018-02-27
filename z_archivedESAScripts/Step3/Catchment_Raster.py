import arcpy
import csv
import datetime
import functions
import os

ClippedUses = True
inLocation = 'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\Refuge\RefugeGAP.gdb'
# rasterLocation = 'H:\Workspace\Step3_Proposal\Yearly_CDL\spe_vegGround.gdb'
results = 'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\Refuge\RefugeOverlapResults'
zonefield = 'Value'
start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

if ClippedUses:
    pathlist = ['C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_corn.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_vegGround.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_orchard.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_cotton.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_rice.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_soybean.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_wheat.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_othergrain.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_otherrow.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_othercrop.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\spe_pasture.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\Clipped_reclass.gdb']
else:
    pathlist = ['C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\NonAg_26.gdb',
                'C:\Users\Admin\Documents\Jen\Workspace\Step3_Proposal\NonAg_11.gdb',

                "C:\Users\Admin\Documents\Jen\Workspace\UseSites\CONUS_NonAg.gdb"]

def CreateDirectory(path_dir):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)

def createdicts(csvfile, key):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname

def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


for HUC2 in functions.rasters_in_workspace(inLocation):
    entidHab = HUC2.split("_")
    entidHab = entidHab[5]
    HUC2path = inLocation + os.sep + HUC2

    folder = HUC2
    print folder

    outgdb =(str(folder) + '.gdb')
    results_FC = results + os.sep + folder
    outpath_final = results_FC + os.sep + outgdb

    CreateDirectory(results_FC)
    CreateGDB(results_FC, outgdb, outpath_final)

    lyrPath = results_FC + os.sep + 'lyr'

    for v in pathlist:
        rasterLocation = v
        for raster in functions.rasters_in_workspace(rasterLocation):
            if ClippedUses:
                entidraster = raster.split("_")
                entidraster = entidraster[4]
                if entidraster != entidHab:
                    continue
                print "Running raster {0} with species {1}".format(entidHab, entidraster)
            else:
                print "Running raster {0} with species {1}".format(raster,entidHab)


            count = 0
            path, flag = os.path.split(rasterLocation)
            flag = flag.replace('.gdb', '')
            runID = flag + "_" + str(raster)
            # print runID
            dem = rasterLocation + os.sep + str(raster)

            start_loop = datetime.datetime.now()
            arcpy.CheckOutExtension("Spatial")

            print "Run to " + str(runID)

            start_loop = datetime.datetime.now()
            dbf = outpath_final + os.sep + str(runID)

            print outpath_final

            if not arcpy.Exists(dbf):
                    arcpy.AddMessage("Start zone...")
                    codezone = functions.ZonalHist(dem, zonefield, HUC2path, dbf, outpath_final)



            print "Run to " + str(runID)

            print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)


