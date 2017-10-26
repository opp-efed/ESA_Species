import datetime
import os

import arcpy

import functions

fieldKeep = ["FileName", "EntityID", "NAME", "Name_sci", "SPCode", "VIPCode", "parID", "SUM", "ID", "Shape_Length",
             "Shape_Area", "Shape_Length", "OBJECTID", "Shape", "Value"]

#inGDB = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\NL48_ProjectedSpGroupComposites.gdb'
inGDB =r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\newComp\NL48_SpGroup_Composites.gdb'
outgdb =r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\newComp\NL48_SpGroup_Composites_Clipped.gdb'
fishnetGDB = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\Fishnets.gdb'
# fishnet = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\Fishnets_NAD83.gdb\PR_10000_StatePlane_NAD83'

skipregion =[]



# outpath_working='J:\Workspace\ESA_Species\ForCoOccur\Results\CriticalHabitat\NL48\GU\Snails\Working.gdb'
# outpath_final='J:\Workspace\ESA_Species\ForCoOccur\Results\CriticalHabitat\NL48\GU\Snails\CH_Snails.gdb'


def CreateDirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


start_script = datetime.datetime.now()
print "Started: {0}".format(start_script)

arcpy.env.workspace = fishnetGDB
fclist = arcpy.ListFeatureClasses()
for fc in functions.fcs_in_workspace(inGDB):
    fc_split = fc.split("_")
    regions = fc_split[0]
    if regions == "PartialLower48":
        regions = 'Lower48Only'
    if regions in skipregion:
        continue
    group = fc_split[2]
    projection = fc_split[4]
    for fish in fclist:
        fish_split = fish.split("_")
        r_region = fish_split[0]
        r_projection = fish_split[2]
        if r_region == regions:
            if r_projection == projection:
                fishnet = fishnetGDB + os.sep + fish
                compFC = inGDB + os.sep + fc
                out= outgdb + os.sep+fc

                print fishnet
                print compFC

                if not arcpy.Exists(out):
                    print "Clipping"
                    arcpy.Clip_analysis(compFC, fishnet, out)
                else:
                    print "already clipped"

            else:
                continue
        else:
            continue

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
