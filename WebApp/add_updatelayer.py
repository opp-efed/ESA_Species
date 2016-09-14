import arcpy
import os
arcpy.env.workspace =r"G:\\Species\\GIS\WebApp\\Fishnet375000\Fishnet_375000_lyr"

inloc =  r"G:\\Species\\GIS\WebApp\\Fishnet375000\Fishnet_375000_lyr"

print "start script"
location =r"G:\\Species\\GIS\\WebApp\\Mxd\\Diaz_AA_fishnet375000.mxd"
outlocation = r"G:\\Species\\GIS\\WebApp\\KMLs"
kmlname= "Diaz_AA_375000_fishnet.kmz"

mxd = arcpy.mapping.MapDocument(location)
df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]

count = 0
layerList = arcpy.ListFiles("*.lyr")
for z in layerList:
    #arcpy.MakeFeatureLayer_management("myFC", "My Layer Name")

    inlyr= inloc +os.sep+str(z)
    addLayer = arcpy.mapping.Layer(inlyr)
    print str(inlyr)
    arcpy.mapping.AddLayer(df, addLayer, "TOP")
    count=count +1
    print count
mxd.save()

sourceLayer = "G:\\Species\\GIS\\WebApp\\FishNet500000\\FishnetLyr_500000\\100_DiazAA.lyr"
layerSymb = arcpy.mapping.Layer(sourceLayer)

count = count-1
#while count < 1363:
while count\
        >=0:
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