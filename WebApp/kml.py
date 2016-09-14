import arcpy
#import os

print "start script"

location =r"G:\\Species\\GIS\\WebApp\\Mxd\\Diaz_AA_fishnet100000.mxd"
outlocation = r"G:\\Species\\GIS\\WebApp\\KMLs"
kmlname= "Diaz_AA_100000_fishnet.kmz"

print "start"

composite = 'NO_COMPOSITE'
#composite = 'COMPOSITE'
dataFrame = 'Layers'
vector = 'VECTOR_TO_VECTOR'
pixels = 2048
dpi = 96
clamped = 'CLAMPED_TO_GROUND'
#outKML = outlocation+os.sep+kmlname
#arcpy.MapToKML_conversion(location, dataFrame, outKML, "1",composite)

#arcpy.MapToKML_conversion(location, dataFrame, outKML, '',composite, '', '', pixels, dpi, clamped)
arcpy.MapToKML_conversion(location, dataFrame, r"G:\\Species\\GIS\\WebApp\\KMLs\\Diaz_AA_100000_fishnet.kmz", '',composite, '', '', pixels, dpi, clamped)
#arcpy.MapToKML_conversion(location, dataFrame, "Z:\\Species\\GIS\\WebApp\\KMLs\\Diaz_AA_final4.kmz")

