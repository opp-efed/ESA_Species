import arcpy
import os
import datetime
import functions

fieldKeep = ["FileName", "EntityID", "NAME", "Name_sci", "SPCode", "VIPCode", "parID", "SUM", "ID", "Shape_Length",
             "Shape_Area", "Shape_Length", "OBJECTID", "Shape", "Value"]

inGDB = 'C:\Users\Admin\Documents\Jen\SpeceisToRun\CH_MissingSpeceis_20151217_L48.gdb'

fishnetGDB = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\Fishnets.gdb'

outpath = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\CriticalHabitat\L48'

skipregions= []




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

    if regions in skipregions:
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


                print fishnet
                print compFC


                outpath_working = outpath + os.sep  + os.sep + group + os.sep + "Working.gdb"
                outpath_final = outpath + os.sep + os.sep + group + os.sep + regions + "_" + group + '.gdb'

                resultfolder, workinggdb = os.path.split(outpath_working)
                finalgdb = os.path.basename(outpath_final)
                lyr = resultfolder + os.sep + 'lyr'

                CreateDirectory(resultfolder)
                CreateDirectory(lyr)

                CreateGDB(resultfolder, workinggdb, (resultfolder + os.sep + workinggdb))
                CreateGDB(resultfolder, finalgdb, (resultfolder + os.sep + finalgdb))

                filename = os.path.basename(compFC)
                outFCSJ = outpath_working + os.sep + filename + '_SJ'

                outFCMB = outpath_working + os.sep + filename + '_MB'
                if arcpy.Exists(outFCSJ) and arcpy.Exists(outFCMB):
                    continue

                print outFCMB
                print outFCSJ


                arcpy.Delete_management("comp")
                arcpy.Delete_management("fish")
                arcpy.Delete_management("SJ")
                arcpy.MakeFeatureLayer_management(compFC, "comp")
                arcpy.MakeFeatureLayer_management(fishnet, "fish")

                arcpy.AddField_management("comp", 'parID', "LONG")
                arcpy.AddField_management("comp", 'SUM', "DOUBLE")

                rows = arcpy.da.UpdateCursor(compFC, ("OBJECTID", "parID"))
                for row in rows:
                    row[1] = row[0]
                    rows.updateRow(row)



                if not arcpy.Exists(outFCSJ):
                    arcpy.SpatialJoin_analysis("comp", fishnet, outFCSJ, "JOIN_ONE_TO_ONE", "KEEP_ALL", "#", "HAVE_THEIR_CENTER_IN", "","")
                else:
                    continue


                if not arcpy.Exists(outFCMB):
                    arcpy.MinimumBoundingGeometry_management(outFCSJ, outFCMB, "RECTANGLE_BY_AREA", "LIST", "ID")

                SJ_listfields = [f.name for f in arcpy.ListFields(outFCSJ)if not f.required]
                MB_listfields = [f.name for f in arcpy.ListFields(outFCMB)if not f.required]

                delfield_SJ = []
                delfield_MB = []
                for f in SJ_listfields:
                    vslue = str(f)
                    if vslue not in fieldKeep:
                        delfield_SJ.append(f)

                for f in MB_listfields:
                    vslue = str(f)
                    if vslue not in fieldKeep:
                        delfield_MB.append(f)

                print delfield_SJ
                print delfield_MB

                if len(delfield_SJ) > 0:
                    arcpy.DeleteField_management(outFCSJ, delfield_SJ)
                if len(delfield_MB) > 0:
                    arcpy.DeleteField_management(outFCMB, delfield_MB)

                outFC_SJ = outpath_final + os.sep + filename + '_SJ'
                outFC_MB = outpath_final + os.sep + filename + '_MB'

                if not arcpy.Exists(outFC_MB):
                    arcpy.CopyFeatures_management(outFCSJ, outFC_SJ)
                    arcpy.CopyFeatures_management(outFCMB, outFC_MB)
                else:
                    continue
            else:
                continue
        else:
            continue

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
