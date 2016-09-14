import arcpy
import os
import sys
import datetime
import shutil

L48 = False
FirstRun = False
ingdb = r'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite.gdb'
# J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\L48_CH_Projected_SpGroupComposites.gdb
outgdb = r'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite_WebMercator.gdb'

proj_Folder = 'J:\Workspace\projections'
desiredProject = 'J:\Workspace\projections\WGS 1984 Web Mercator (auxiliary sphere).prj'
prjABB = 'WebMercator'
templocation = r'C:\Workspace\temp\temp2.gdb'
# Index postion of the sp group name within the file name
groupindex = 1
regionindex = 0


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


def CreateDirectory(path_dir):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

group = []
regions = []

if not os.path.exists(outgdb):
    path, gdb = os.path.split(outgdb)
    CreateGDB(path, gdb, outgdb)

path, gdb = os.path.split(templocation)

tempgdb = templocation
if FirstRun:
    if os.path.exists(templocation):
        sys.exit("Choose a different temp gdb name")

CreateDirectory(path)
CreateGDB(path, gdb, templocation)

desireddsc = arcpy.Describe(desiredProject)
descoord_sys = desireddsc.spatialReference
desired_type = desireddsc.spatialReference.Type
desired_prj = descoord_sys.name.lower()
desireddatum = descoord_sys.GCS.datumName

coordFile = proj_Folder + os.sep + 'NAD 1983.prj'
dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

WGScoordFile = proj_Folder + os.sep + 'WGS 1984.prj'
dscwgs = arcpy.Describe(WGScoordFile)
wgscoord_sys = dscwgs.spatialReference

if desireddatum == "D_North_American_1983":
    abb = "NAD83"
    nonabb = 'WGS84'
    Coord = coord_sys
elif desireddatum == "D_WGS_1984":
    abb = "WGS84"
    nonabb = "NAD83"
    Coord = wgscoord_sys
else:
    sys.exit("Datum not accounted for")

unknownprj = []

arcpy.env.workspace = outgdb
fclist = arcpy.ListFeatureClasses()
for fc in fclist:
    splitname = fc.split("_")
    currentgroup = splitname[groupindex]
    region = splitname[regionindex]
    if L48:
        group.append(currentgroup)
    else:
        currentgroup = currentgroup + "_" + region
        group.append(currentgroup)

print group

arcpy.env.workspace = ingdb
fclist = arcpy.ListFeatureClasses()
totalfc = len(fclist)
counter = 1
for fc in fclist:
    start_loop = datetime.datetime.now()
    splitname = fc.split("_")
    currentgroup = splitname[groupindex]
    region = splitname[regionindex]
    print "\nWorking on {0} file {1} of {2}".format(fc, counter, totalfc)
    if L48:
        if currentgroup in group:
            counter += 1
            print "Already projected files for {0}".format(currentgroup)
            continue
        else:
            group.append(currentgroup)
    else:
        currentgroup = currentgroup + "_" + region
        if currentgroup in group:
            counter += 1
            currentgroup = currentgroup.split("_")
            print "Already projected files for {0} in {1}".format(currentgroup[0], currentgroup[1])
            continue
        else:
            group.append(currentgroup)

    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    if ORGdsc.spatialReference.Type == "Projected":
        infc = ingdb + os.sep + fc
        if desired_type == "Projected":
            if desired_prj == ORGprj:
                print "File is in correct project projection, copying features"
                outfc = outgdb + os.sep + (fc + "_" + prjABB)
                arcpy.CopyFeatures_management(infc, outfc)
            else:
                ORGdataum = ORGsr.GCS.datumName
                if desireddatum == ORGdataum:
                    geofc = fc + "_" + abb
                    tempgeo = tempgdb + os.sep + geofc
                    outfc = outgdb + os.sep + (fc + "_" + prjABB)
                    if not arcpy.Exists(outfc):
                        if not arcpy.Exists(tempgeo):
                            print "Generating temporary final geographic projected file... "
                            arcpy.Project_management(infc, tempgeo, ORGsr)
                        print "Generating final projected FC..."
                        arcpy.Project_management(tempgeo, outfc, descoord_sys)
                    del geofc, tempgeo
                else:
                    geofc = fc + "_" + nonabb
                    tempgeo = tempgdb + os.sep + geofc
                    geodesired = geofc + "_" + abb
                    tempgeodesired = tempgdb + os.sep + geodesired
                    outfc = outgdb + os.sep + (geodesired + "_" + prjABB)

                    if not arcpy.Exists(outfc):
                        if not arcpy.Exists(tempgeo):
                            print "Generating temporary original geographic projected file... "
                            arcpy.Project_management(infc, tempgeo, ORGsr)
                        if not arcpy.Exists(tempgeodesired):
                            print "Generating temporary final geographic projected file... "
                            arcpy.Project_management(tempgeo, tempgeodesired, Coord)
                        print "Generating final projected FC..."
                        arcpy.Project_management(tempgeodesired, outfc, descoord_sys)
                    del tempgeo, tempgeodesired

        else:
            ORGdataum = ORGsr.GCS.datumName
            if desireddatum == ORGdataum:
                outfc = outgdb + os.sep + (fc + "_" + prjABB)
                if not arcpy.Exists(outfc):
                    arcpy.Project_management(infc, outfc, descoord_sys)
            else:
                geofc = fc + "_" + abb
                tempfc = tempgdb + os.sep + geofc
                outfc = outgdb + os.sep + (geofc + "_" + prjABB)
                if not arcpy.Exists(outfc):
                    if not arcpy.Exists(tempfc):
                        print "Generating temporary final geographic projected file... "
                        arcpy.Project_management(infc, tempfc, Coord)
                    print "Generating final geographic FC..."
                    arcpy.Project_management(tempfc, outfc, descoord_sys)
                del tempfc


    elif ORGdsc.spatialReference.Type == "Geographic":
        infc = ingdb + os.sep + fc
        if desired_type == "Geographic":
            if desired_prj == ORGprj:
                outfc = outgdb + os.sep + (fc + "_" + prjABB)
                print "File is in correct geographic projection, copying features"
                arcpy.CopyFeatures_management(infc, outfc)
            else:
                ORGdataum = ORGsr.GCS.datumName
                if desireddatum == ORGdataum:
                    outfc = outgdb + os.sep + (fc + "_" + prjABB)
                    if not arcpy.Exists(outfc):
                        arcpy.Project_management(infc, outfc, descoord_sys)
                else:
                    geofc = fc + "_" + abb
                    tempfc = tempgdb + os.sep + geofc
                    outfc = outgdb + os.sep + (geofc + "_" + prjABB)
                    if not arcpy.Exists(outfc):
                        if not arcpy.Exists(tempfc):
                            print "Generating temporary final geographic projected file... "
                            arcpy.Project_management(infc, tempfc, Coord)
                        print "Generating final geographic FC..."
                        arcpy.Project_management(tempfc, outfc, descoord_sys)
                    del tempfc

        else:

            ORGdataum = ORGsr.GCS.datumName
            if desireddatum == ORGdataum:
                geofc = fc + "_" + abb
                tempgeo = tempgdb + os.sep + geofc
                outfc = outgdb + os.sep + (fc + "_" + prjABB)
                if not arcpy.Exists(outfc):
                    print "Generating temporary final geographic projected file... "
                    if not arcpy.Exists(tempgeo):
                        arcpy.Project_management(infc, tempgeo, ORGsr)
                    print "Generating final projected FC..."
                    arcpy.Project_management(tempgeo, outfc, descoord_sys)
                del tempgeo
            else:
                geofc = fc + "_" + nonabb
                tempgeo = tempgdb + os.sep + geofc
                geodesired = geofc + "_" + abb

                tempgeodesired = tempgdb + os.sep + geodesired
                outfc = outgdb + os.sep + (geodesired + "_" + prjABB)

                if not arcpy.Exists(outfc):
                    if not arcpy.Exists(tempgeo):
                        print "Generating temporary original geographic projected file... "
                        arcpy.Project_management(infc, tempgeo, ORGsr)
                    if not arcpy.Exists(tempgeodesired):
                        print "Generating temporary final geographic projected file... "
                        arcpy.Project_management(tempgeo, tempgeodesired, Coord)
                    print "Generating final projected FC..."
                    arcpy.Project_management(tempgeodesired, outfc, descoord_sys)
                del tempgeo, tempgeodesired

    else:
        unknownprj.append(fc)
        print "Failed to add filename " + str(fc)
    counter += 1
    endloop = datetime.datetime.now()
    print "Elapse time for {1} was {0}".format((endloop - start_loop), fc)

print "Files with unknown projections {0}".format(unknownprj)

while True:
    user_input = raw_input('Do you want to delete temp files Yes or No: ')
    if user_input in ['Yes', 'No']:
        break
    else:
        print('\nThat is not a valid option! Type Yes or No')

if user_input == 'Yes':
    tempfiles = os.listdir(path)
    for v in tempfiles:
        delfile = os.path.join(path, v)
        if os.path.exists(delfile):
            shutil.rmtree(delfile)
            print "deleting {0}".format(delfile)
    print "deleting {0}".format(path)
    if os.path.exists(path):
        shutil.rmtree(path)
elif user_input == 'No':
    print "Temp files found at {0}".format(path)
else:
    print 'Del failed'

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
