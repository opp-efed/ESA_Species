# import system modules
import os
import csv
import time
import datetime
import functions

import arcpy

# Title : Checks projection for all fc infolder to make sure they are in the STD geo projection before moving forward

Infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'

# #####################################################################################################################
##############################################################################################################
start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)
# Prints Elapse clock
prj_list = []

for fc in functions.fcs_in_workspace(Infolder):
    # set local variables
    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    if ORGprj not in prj_list:
        prj_list.append(ORGprj)
        print "{0}  {1}".format(fc, ORGsr.name)
    else:
        pass
