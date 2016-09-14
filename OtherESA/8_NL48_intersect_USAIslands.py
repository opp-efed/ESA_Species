import arcpy
import PrepFiles_CoOccur.functions
import datetime
import os

masterlist = r'C:\Users\Admin\Documents\Jen\Workspace\MasterLists\CSV\MasterListESA_April2015_20151015_20151118.csv'

inFishnets = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\MissingIslands.gdb'

infolder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\CriticalHabitat\NAD83_SinglePart'
outfolder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\CriticalHabitat\MissingIslands'
regionsgdb_csv = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\regionsgdb_Missing.csv'

skiplist = []


def CreateDirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


start_script = datetime.datetime.now()
print "Started: {0}".format(start_script)
grouplist = []
Wake = []
PR = []
Palmyra = []
Johnston = []
HI = []
Howland = []
AS = []

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


for group in alpha_group:
    if group in skiplist:
        continue

    print "\nWorking on {0}".format(group)

    for fish in PrepFiles_CoOccur.functions.fcs_in_workspace(inFishnets):
        print "Working with {0}".format(fish)

        arcpy.Delete_management("fish_lyr")
        infish = inFishnets + os.sep + fish
        arcpy.MakeFeatureLayer_management(infish, "fish_lyr")

        resultfolder = outfolder + os.sep + group
        CreateDirectory(resultfolder)

        regionsDIR = resultfolder + os.sep + "Regions"
        CreateDirectory(regionsDIR)

        NAD83DIR = regionsDIR + os.sep + 'NAD83'
        CreateDirectory(NAD83DIR)

        region = str(fish)
        with open(regionsgdb_csv, 'rU') as inputFile2:
            for line in inputFile2:
                line = line.split(',')
                fishnet = str(line[0])
                if fishnet == fish:
                    outgbdname = str(line[1])
                    list = str(line[2])
                    list=list.strip("\n")
                else:
                    continue
        inputFile2.close()

        regionGDB = outgbdname
        outpathgdb = NAD83DIR + os.sep + regionGDB

        if not arcpy.Exists(outpathgdb):
            CreateGDB(NAD83DIR, regionGDB, outpathgdb)


        inGDB = infolder + os.sep + group + '.gdb'
        print inGDB
        for fc in PrepFiles_CoOccur.functions.fcs_in_workspace(inGDB):

                arcpy.Delete_management("fc_lyr")
                infc = inGDB + os.sep + fc
                arcpy.MakeFeatureLayer_management(infc, "fc_lyr")
                arcpy.SelectLayerByLocation_management("fc_lyr", 'intersect', "fish_lyr", "#", "NEW_SELECTION")
                arcpy.Delete_management("sel_lyr")
                arcpy.MakeFeatureLayer_management("fc_lyr", "sel_lyr")
                count = int(arcpy.GetCount_management("sel_lyr").getOutput(0))
                if count > 0:
                    outfc = outpathgdb + os.sep + region + "_" + str(fc)
                    if not arcpy.Exists(outfc):
                        arcpy.CopyFeatures_management("fc_lyr", outfc)
                        vars()[list].append(fc)
                        print "Export regional file for {0}".format(fc)
                else:
                    continue
print "species in Wake {0}".format(Wake)
print "species in AS {0}".format(AS)
print "species in Palmyra {0}".format(Palmyra)
print "species in Johnston  {0}".format(Johnston)
print "species in HI {0}".format(HI)
print "species in Howland {0}".format(Howland)
print "species in PR {0}".format(PR)



print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
