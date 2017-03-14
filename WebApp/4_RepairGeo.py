
# Author: ESRI

import arcpy
import os
import functions



infolder ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH\WebMercator'


for fc in functions.fcs_in_workspace(infolder):
        try:
            print "\nProcessing " + fc
            lyr = 'temporary_layer'

            arcpy.MakeFeatureLayer_management(fc, lyr)
            arcpy.RepairGeometry_management(lyr)
            print(arcpy.GetMessages(0)) # change this to 1 to receive just the warning
            arcpy.Delete_management(lyr)
        except Exception as error:
            print(error.args[0])


