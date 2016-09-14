
# Author: ESRI

import arcpy
import os
import functions



infolder ='J:\Workspace\ESA_Species\Range\NAD83\Reptiles.gdb'


for fc in functions.fcs_in_workspace(infolder):
        print "\nProcessing " + fc
        lyr = 'temporary_layer'

        arcpy.MakeFeatureLayer_management(fc, lyr)
        arcpy.RepairGeometry_management(lyr)
        print(arcpy.GetMessages(0)) # change this to 1 to receive just the warning
        arcpy.Delete_management(lyr)
