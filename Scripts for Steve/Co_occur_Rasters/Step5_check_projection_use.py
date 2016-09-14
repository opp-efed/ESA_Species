# import system modules
import os
import csv
import time
import datetime

import arcpy

# Title - Check projection of all use to make sure all projections accounted for in steps 7
InGDB = 'J:\Workspace\UseSites\NonCONUS_AG_151109.gdb'

# recursively checks workspaces found within the inFileLocation and makes list of all rasters
def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster


# #####################################################################################################################

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


for raster in rasters_in_workspace(InGDB):
    # extracts spatial info for each raster
    ORGdsc = arcpy.Describe(raster)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()

    print "{0}  {1}".format(raster, ORGsr.name)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
