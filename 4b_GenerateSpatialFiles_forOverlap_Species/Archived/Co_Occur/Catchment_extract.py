import os
import gc
import time
import datetime
import csv
import functions
import arcpy

gc.enable()


# ## to do create streams layers by huc (huc_fc) onces then open it rather than making it for each species
# ##possibly speed up by only extracting information need from table ie reach code, save to a list, append list then
### write to a CSV at end for each rather than using table to table conversion
###add code to export what is "print" to a txt

inLocations ='H:\NHDPlusV2'
outLocations = 'H:\Workspace\ESA_Species\Catchment'

def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")

start_script =datetime.datetime.now()
print "Script started at: {0}".format(start_script)



listFolders = os.listdir(inLocations)

for v in listFolders:
    if v[:3]!= 'NHD':
        listFolders.remove(v)
        continue
    else:
        catchment = inLocations+ os.sep + v + os.sep+ 'NHDPlusCatchment\Catchment.shp'
        outgdb= 'CatchmentRaw.gdb'
        outpath = outLocations +os.sep + outgdb
        CreateGDB(outLocations,outgdb, outpath)
        outcatchment= outpath+ os.sep + v+ "_catchment"

        if not arcpy.Exists(outcatchment):
            arcpy.CopyFeatures_management(catchment,outcatchment )
            arcpy.AddField_management(outcatchment,"SUM","DOUBLE")
            print " print copied raw catchment file {0}".format(v+ "_catchment")


print listFolders








