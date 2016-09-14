import arcpy
import os
import datetime

masterlist = 'J:\Workspace\MasterLists\CSV\MasterListESA_April2015_20151015_20151118.csv'


ingdb = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\Fishnets.gdb'
outgdb = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\Fishnets_NAD83.gdb'
middlegdb = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\Fishnetsmiddle.gdb'

proj_Folder = 'J:\Workspace\projections'


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

grouplist = []
NADabb = "NAD83"

coordFile = proj_Folder + os.sep + 'NAD 1983.prj'
WGScoordFile = proj_Folder + os.sep + 'WGS 1984.prj'

dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

dscwgs = arcpy.Describe(WGScoordFile)
wgscoord_sys = dscwgs.spatialReference

unknownprj = []
group_gdb = ingdb

arcpy.env.workspace = group_gdb
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    infc = group_gdb + os.sep + fc
    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    if ORGdsc.spatialReference.Type == "Projected":
        print "Projected Conversion"
        # print ORGsr.GCS.datumName
        if ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD
            if arcpy.Exists(outfc):
                print str(fc) + " already exists"
                continue
            else:
                arcpy.Project_management(infc, outfc, coord_sys)
                print "completed {0}".format(fc)

        elif ORGsr.GCS.datumName == "D_WGS_1984":
            fcotherGEO = str(fc) + "_WGS84"
            outotherfc = middlegdb + os.sep + fcotherGEO

            fcNAD = str(fcotherGEO) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD

            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(infc, outotherfc, wgscoord_sys)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"
        else:
            unknownprj.append(fc)
            print "Unknown projection"
    elif ORGdsc.spatialReference.Type == "Geographic":
        print "Geographic Conversion"
        if ORGsr.GCS.datumName == "D_WGS_1984":
            fcotherGEO = str(fc) + "_WGS84"
            outotherfc = middlegdb + os.sep + fcotherGEO

            fcNAD = str(fcotherGEO) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD

            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(infc, outotherfc, wgscoord_sys)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"

        if ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD
            if not arcpy.Exists(outfc):
                arcpy.Project_management(fc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"

    else:
        unknownprj.append(fc)
        print "Failed to add filename " + str(fc)

print "Failed fcs {0}".format(unknownprj)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
