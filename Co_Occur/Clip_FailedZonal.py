import arcpy, traceback, os, sys, csv, time, datetime
from arcpy import env
import functions

print "start"


inFolder ='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\ResultsByUse\\test\Final.gdb'
raster ='C:\Users\Admin\Documents\Jen\Workspace\UseSites\CONUS_USA_NonAg.gdb\CONUS_ManagedForests'

out_working = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\ResultsByUse\\test\Working.gdb'

def fcs_in_workspace(workspace):
  arcpy.env.workspace = workspace
  for fc in arcpy.ListFeatureClasses():
    yield(fc)
  for ws in arcpy.ListWorkspaces():
    for fc in fcs_in_workspace(ws):
        yield fc

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

for fc in fcs_in_workspace(inFolder):
    print fc
    with arcpy.da.SearchCursor (fc,["EntityID", "SUM"])as cursor:
        for row in cursor:
            if row[1]>-1:
                continue
            else:
                arcpy.env.overwriteOutput=True
                entid = row[0]
                print entid
                whereclause = "EntityID = '%s'" % (entid)
                arcpy.Delete_management("fc_lyr")
                arcpy.MakeFeatureLayer_management(fc,"fc_lyr", whereclause)
                for row in arcpy.da.SearchCursor("fc_lyr", ["SHAPE@", "EntityID"]):
                    extent = row[0].extent
                    Xmin = extent.XMin
                    Ymin = extent.YMin
                    Xmax = extent.XMax
                    Ymax= extent.YMax
                    extent_layer = str(Xmin) + " " + str(Ymin) + " " + str(Xmax)+ " " + str(Ymax)
                    print extent_layer
                    outraster =out_working+os.sep+"clip_"+entid+ "_"+str(fc)
                    #outraster = out_working+os.sep+str(raster)+ '_' +str(entid)
                    arcpy.Clip_management (raster,extent_layer,outraster,"fc_lyr","2147483647", "ClippingGeometry", "NO_MAINTAIN_EXTENT")
                    #arcpy.Clip_management (raster,extent_layer,outraster,"fc_lyr","0","NONE", "MAINTAIN_EXTENT")
                    arcpy.BuildRasterAttributeTable_management(outraster, "Overwrite")
                    count = int(arcpy.GetCount_management(outraster).getOutput(0))
                    if count == 0:
                        with arcpy.da.UpdateCursor(fc, ("EntityID","SUM")) as update:
                                for line in update:
                                    if line[0]==entid:
                                            line[1]= 0
                                            cursor.updateRow(row)
                                            continue
                                    else:
                                            continue
                        del update,line

                    else:
                            with arcpy.da.SearchCursor(outraster, ["Value","Count"]) as cursor:
                                for row in cursor:
                                    if row[0] == 1:
                                        sum = row[1]
                                        print row[0]
                                        print sum

                                        with arcpy.da.UpdateCursor(fc, ("EntityID","SUM")) as update:
                                            for line in update:
                                                if line[0]==entid:
                                                    line[1]= sum
                                                    update.updateRow(line)
                                                    continue
                                                else:
                                                    continue
                                        del update,line
                                    else:
                                        continue
                            del row, cursor