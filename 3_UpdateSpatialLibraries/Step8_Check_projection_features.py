# import system modules
import os
import csv
import time
import datetime
import functions

import arcpy

# Author J.Connolly
# Internal deliberative, do not cite or distribute
# Title : Checks projection for all fc in folder to make sure they are in the STD geo projection before moving forward
# TODO - add in the path of the file a different projection- script is picking up archived files

# Generalized CH or Range files to be used as generated the composite input files for overlap- generalized or non
infolder = 'path\Range'

# #####################################################################################################################
##############################################################################################################
start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)
# Prints Elapse clock
prj_list = []

for fc in functions.fcs_in_workspace(infolder):

    # set local variables
    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    if ORGprj not in prj_list:
        prj_list.append(ORGprj)
        print "{0}  {1}".format(fc, ORGsr.name)
    else:
        pass
