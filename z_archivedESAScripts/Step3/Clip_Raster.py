import arcpy
import datetime
import os

import functions

SingleRaster = False #one raster will be cliped or a group in a GDB
SingleFC = True #one species or many
speciesRangefc = True #species range or critical habitat

out_location= 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\Overlapping_UseRaster_Combine\Work_570_CONUS.gdb'
#out_working = 'J:\Workspace\Step3_Proposal\Yearly_CDL\SpeRangeClips.gdb'
inraster = r'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb'
#J:\Workspace\Step3_Proposal\Yearly_CDL\CDL_reclass_new.gdb
infc = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\Overlapping_UseRaster_Combine\Work_570_CONUS.gdb\R_953_CONUS'
#snapRaster = r"L:\Workspace\ESA_Species\Step3\Step3_Proposal\GAP\National\natgaplandcov_v2_2_1.img"
snapRaster = r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img"
arcpy.MakeRasterLayer_management(snapRaster,"snap")
arcpy.env.snapRaster = "snap"

def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")
    return outpath


start_script = datetime.datetime.now()

print "Script started at: {0}".format(start_script)

if SingleFC:
    path,tail = os.path.split(out_location)
    path_fc, tail_fc = os.path.split(infc)
    print tail[-3:]
    if tail[-3:] == 'gdb':
        out_working=out_location
    else:
        outname = tail_fc+'.gdb'
        out_working = CreateGDB(out_location,(outname),(out_location+os.sep+outname))
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(infc, "fc_lyr")
    if speciesRangefc:
        fc_fieldlist=["SHAPE@", "EntityID","Region"]
    else:
        fc_fieldlist= ["SHAPE@"]

    for row in arcpy.da.SearchCursor("fc_lyr", fc_fieldlist):
        if row[2]!= 'Lower48':
            continue
        elif speciesRangefc:
            entid = row[1]
            extent = row[0].extent
        else:
            extent = row[0].extent
        Xmin = extent.XMin
        Ymin = extent.YMin
        Xmax = extent.XMax
        Ymax = extent.YMax
        extent_layer = str(Xmin) + " " + str(Ymin) + " " + str(Xmax) + " " + str(Ymax)
        print extent_layer
    if SingleRaster == True:
        print "single raster {0}".format(inraster)
        path, raster = os.path.split(inraster)
        raster = raster.split('.')
        raster = raster[0]
        if speciesRangefc:
            outraster = out_working + os.sep + raster + "_clip_" + entid
            print outraster
        else:
            path,fcname = os.path.split(infc)
            outraster = out_working + os.sep + raster + "_clip_" + fcname
            print outraster
        if arcpy.Exists(outraster):
            print "Print already complete {0}".format(outraster)
        arcpy.env.workspace = out_working
        arcpy.env.overwriteOutput = True
        # outraster = out_working+os.sep+str(raster)+ '_' +str(entid)
        arcpy.Clip_management(inraster, extent_layer, outraster, "fc_lyr", "2147483647",
                              "ClippingGeometry", "NO_MAINTAIN_EXTENT")
    else:
        for raster in functions.rasters_in_workspace(inraster):
            print raster
            start_loop = datetime.datetime.now()
            if speciesRangefc:
                outraster = out_working + os.sep + raster + "_clip_" + entid
                print outraster
            else:
                path,fcname = os.path.split(infc)
                outraster = out_working + os.sep + raster + "_clip_" + fcname
                print outraster
            if arcpy.Exists(outraster):
                print "Print already complete {0}".format(outraster)
                continue
            arcpy.env.workspace = out_working
            arcpy.env.overwriteOutput = True
            inloopraster = inraster + os.sep + raster
            # outraster = out_working+os.sep+str(raster)+ '_' +str(entid)
            arcpy.Clip_management(inloopraster, extent_layer, outraster, "fc_lyr", "2147483647",
                                  "ClippingGeometry", "NO_MAINTAIN_EXTENT")
            print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)
else:
    path_fc, tail_fc = os.path.split(infc)

    for fc in functions.fcs_in_workspace(infc):
        path,tail = os.path.split(out_location)

        if tail[-3:] == 'gdb':
            out_working=out_location
        else:
            outname =tail_fc
            out_working = CreateGDB(out_location,(outname),(out_location+os.sep+outname))
        print fc
        inloopfc =infc +os.sep +fc
        arcpy.Delete_management("fc_lyr")
        arcpy.MakeFeatureLayer_management(inloopfc, "fc_lyr")
        for row in arcpy.da.SearchCursor("fc_lyr", ["SHAPE@", "EntityID"]):
            if speciesRangefc:
                entid = row[1]
                extent = row[0].extent
                print entid
            else:
                extent = row[0].extent
            Xmin = extent.XMin
            Ymin = extent.YMin
            Xmax = extent.XMax
            Ymax = extent.YMax
            extent_layer = str(Xmin) + " " + str(Ymin) + " " + str(Xmax) + " " + str(Ymax)
            print extent_layer
            if SingleRaster:
                print "single raster {0}".format(inraster)
                path, raster = os.path.split(inraster)
                raster = raster.split('.')
                raster = raster[0]
                if speciesRangefc:
                    outraster = out_working + os.sep + raster + "_clip_" + entid
                    print outraster
                else:
                    path,fcname = os.path.split(infc)
                    outraster = out_working + os.sep + raster + "_clip_" + fcname
                    print outraster
                if arcpy.Exists(outraster):
                    print "Print already complete {0}".format(outraster)
                    continue
                arcpy.env.workspace = out_working
                arcpy.env.overwriteOutput = True
                arcpy.Clip_management(inraster, extent_layer, outraster, "fc_lyr", "2147483647",
                                      "ClippingGeometry", "NO_MAINTAIN_EXTENT")
            else:
                for raster in functions.rasters_in_workspace(inraster):
                    print raster
                    start_loop = datetime.datetime.now()
                    if speciesRangefc:
                        outraster = out_working + os.sep + raster + "_clip_" + entid
                        print outraster
                    else:
                        path,fcname = os.path.split(infc)
                        outraster = out_working + os.sep + raster + "_clip_" + fcname
                        print outraster
                    if arcpy.Exists(outraster):
                        print "Print already complete {0}".format(outraster)
                        continue
                    inloopraster = inraster + os.sep + raster

                    arcpy.Clip_management(inloopraster, extent_layer, outraster, "fc_lyr", "2147483647",
                                          "ClippingGeometry", "NO_MAINTAIN_EXTENT")

                    print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
