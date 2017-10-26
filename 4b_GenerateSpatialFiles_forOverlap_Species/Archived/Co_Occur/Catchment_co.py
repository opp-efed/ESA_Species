import csv
import datetime
import gc
import os

import arcpy

import functions

gc.enable()

inLocation = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\Catchment\CatchmentRaw.gdb'
rasterLocation ='C:\Users\Admin\Documents\Jen\Workspace\UseSites\diazinon_150420.gdb'
exportdict = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\NewLayersDict_export.csv"


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

def createdicts(csvfile, key):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname

export_dict = {}
export_dict = createdicts(exportdict, export_dict)
for k in export_dict:
    value = export_dict[k]

for fc in functions.fcs_in_workspace(inLocation):
    workinggdb =''
    minBound= ''
    catchment = outcatchment
    outpath_final = ''
    lyrPath = ''
    for raster in functions.rasters_in_workspace(rasterLocation):
        count =0
        flag= fc
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
        else:
            start_loop = datetime.datetime.now()
            functions.CoOccur(workinggdb, runID, minBound, catchment, lyrPath, raster, dem)
            print "Run to " + str(runID)

            outFC = outpath_final + os.sep + runID
            desc = arcpy.Describe(catchment)
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


            with arcpy.da.UpdateCursor(catchment, ("SUM")) as cursor:
                for row in cursor:
                    if row[0] > -2:
                        row[0] = None
                        cursor.updateRow(row)


        #del raster,codezone
        print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)

