import arcpy
import os
arcpy.env.workspace = r"Z:\Species\GIS\WebApp\FishNet_LYR"

inloc = r"Z:\Species\GIS\WebApp\FishNet_LYR"

print "start script"
location =r"Z:\\Species\\GIS\\WebApp\\Mxd\\Diaz_AA_fishent3.mxd"
outlocation = r"Z:\\Species\\GIS\\WebApp\\KMLs"
kmlname= "Diaz_AA_final_fishnet3.kmz"


sourceLayer = "Z:\\Species\\GIS\\WebApp\\DiazAA_Lyr\\WY_DiazAA.lyr"
layerSymb = arcpy.mapping.Layer(sourceLayer)

mxd = arcpy.mapping.MapDocument(location)
df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]

count = 0
layerList = arcpy.ListFiles("*.lyr")
for z in layerList:
    #arcpy.MakeFeatureLayer_management("myFC", "My Layer Name")

    inlyr= inloc +os.sep+str(z)
    addLayer = arcpy.mapping.Layer(inlyr)
    #arcpy.AddColormap_management(addLayer, "#", "Z:\Species\GIS\WebApp\colormaps\diaz_AA.clr")
    print str(inlyr)
    arcpy.mapping.AddLayer(df, addLayer, "TOP")
    count=count +1
    print count
mxd.save()


count = count-1
#while count < 1363:
while count >=0:
    updateLayer = arcpy.mapping.ListLayers(mxd)[count]
    arcpy.mapping.UpdateLayer(df, updateLayer, layerSymb, "TRUE")
    print count
    count = count -1
arcpy.RefreshTOC()
mxd.save()
del mxd


composite = 'NO_COMPOSITE'
dataFrame = 'Layers'
outKML = outlocation+os.sep+kmlname
arcpy.MapToKML_conversion(location, dataFrame, outKML, "1",composite)