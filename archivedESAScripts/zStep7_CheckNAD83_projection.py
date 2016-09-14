import arcpy
import os
import datetime

masterlist = 'J:\Workspace\ESA_Species\ForCoOccur\Documents_FinalBE\MasterListESA_June2016_20160725.csv'

infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD83'
outfolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'
middlegdb = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD83\middle.gdb'


proj_Folder = 'J:\Workspace\projections'

#Species groups that do not need to be run
skiplist = ['Amphibians', 'Arachnids', 'Birds']

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

with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        entid = line[0]
        group = line[1]
        grouplist.append(group)

inputFile.close()

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)

coordFile = proj_Folder + os.sep + 'NAD 1983.prj'
WGScoordFile = proj_Folder + os.sep + 'WGS 1984.prj'

dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

dscwgs = arcpy.Describe(WGScoordFile)
wgscoord_sys = dscwgs.spatialReference

unknownprj = []
for group in alpha_group:
    if group in skiplist:
        continue
    print "\nWorking on {0}".format(group)

    group_gdb = infolder + os.sep + str(group) + '.gdb'
    outname = str(group) + '.gdb'
    outgdb = outfolder + os.sep + outname
    CreateGDB(outfolder, outname, outgdb)

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    total = len(fclist)
    for fc in fclist:

        infc = group_gdb + os.sep + fc
        ORGdsc = arcpy.Describe(fc)
        ORGsr = ORGdsc.spatialReference
        ORGprj = ORGsr.name.lower()
        if ORGdsc.spatialReference.Type == "Projected":
            #print "Projected Conversion"
            # print ORGsr.GCS.datumName
            if ORGsr.GCS.datumName == "D_North_American_1983":
                fcNAD = str(fc) + "_" + str(NADabb)
                outfc = outgdb + os.sep + fcNAD
                if arcpy.Exists(outfc):
                    total -= 1
                    print str(fc) + " already exists"
                    continue
                else:
                    arcpy.Project_management(infc, outfc, coord_sys)
                    total -= 1
                    print "completed {0} {1} remaining in {2}".format(fc, total, group)

            elif ORGsr.GCS.datumName == "D_WGS_1984":
                fcotherGEO = str(fc) + "_WGS84"
                outotherfc = middlegdb + os.sep + outotherfc

                fcNAD = str(fcotherGEO) + "_" + str(NADabb)
                outfc = outgdb + os.sep + fcNAD

                if not arcpy.Exists(outotherfc):
                    arcpy.Project_management(infc, outotherfc, wgscoord_sys)

                if not arcpy.Exists(outfc):
                    arcpy.Project_management(outotherfc, outfc, coord_sys)
                    total -= 1
                    print "completed {0} {1} remaining in {2}".format(fc, total, group)
                else:
                    total -= 1
                    print str(fc) + " already exists"
            else:
                unknownprj.append(fc)
                print "Unknown projection"

        elif ORGdsc.spatialReference.Type == "Geographic":
            #print "Geographic Conversion"
            if ORGsr.GCS.datumName == "D_WGS_1984":
                fcotherGEO = str(fc) + "_WGS84"
                outotherfc = middlegdb + os.sep + outotherfc

                fcNAD = str(fcotherGEO) + "_" + str(NADabb)
                outfc = outgdb + os.sep + fcNAD

                if not arcpy.Exists(outotherfc):
                    arcpy.Project_management(infc, outotherfc, wgscoord_sys)

                if not arcpy.Exists(outfc):
                    arcpy.Project_management(outotherfc, outfc, coord_sys)
                    total -= 1
                    print "completed {0} {1} remaining in {2}".format(fc, total, group)
                else:
                    total -= 1
                    print str(fc) + " already exists"

            if ORGsr.GCS.datumName == "D_North_American_1983":
                fcNAD = str(fc) + "_" + str(NADabb)
                outfc = outgdb + os.sep + fcNAD
                if not arcpy.Exists(outfc):
                    arcpy.Project_management(fc, outfc, coord_sys)
                    total -= 1
                    print "completed {0} {1} remaining in {2}".format(fc, total,group)
                else:
                    total -= 1
                    print str(fc) + " already exists"

        else:
            unknownprj.append(fc)
            total -= 1
            print "Failed to add filename " + str(fc)

print "Failed fcs {0}".format(unknownprj)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
