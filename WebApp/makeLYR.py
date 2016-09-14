import os

import arcpy
arcpy.env.workspace = r"G:\Temp"
outlocation= r"G:\\Species\\GIS\WebApp\\Fishnet375000\Fishnet_375000_lyr"
shplocation= r"G:\\Species\\GIS\WebApp\\Fishnet375000\\Fishnet_375000_shp"
fc= "G:\\Fishnest_Co-Occur\\Co-Occur.gdb\\fishnet_375000_ProjectWGSb"
raster= "G:\\Species\\GIS\\WebApp\\FishNet500000\\Default.gdb\diazinon_150420_WGS_ProjectR"
idLyr = "_DiazAA"

#arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
sc = arcpy.da.SearchCursor(fc, ["netID"])

for row in sc:
    arcpy.Delete_management("lyr")
    arcpy.Delete_management("rdlayer")
    id = row[0]
    print str(id)
    extent = shplocation+ os.sep + str(id)+ "_fc"+".shp"

    #arcpy.SaveToLayerFile_management("lyr", out_fc, "ABSOLUTE")
    ##Create raster layer from single raster dataset with clipping feature

    try:
        whereraster = "Value = 1"
        arcpy.MakeRasterLayer_management(raster, "rdlayer", whereraster, extent)
        out_layer = outlocation+ os.sep + str(id)+ idLyr
        arcpy.SaveToLayerFile_management("rdlayer", out_layer, "ABSOLUTE")
        if id != "CA":
            continue
        else:
            extent2 = shplocation+ os.sep + str(id)+ "Island_fc"+".shp"
            arcpy.MakeRasterLayer_management(raster, "rdlayer", whereraster, extent2)
            out_layer2 = outlocation+ os.sep + str(id)+ "Island"+ idLyr
            arcpy.SaveToLayerFile_management("rdlayer", out_layer2, "ABSOLUTE")

    except:
        print "Make Raster Layer example failed."
        print arcpy.GetMessages()
