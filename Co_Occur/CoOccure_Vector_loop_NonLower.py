# #  Zonal statistics on a set of overlapping
import arcpy, traceback, os, sys, csv, time, datetime
from arcpy import env
import functions

parID = "parID"
parID2 = "parID_1"

group = 'Plants'
region = 'HI_'
fileType = 'R'

fc= 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\NonL48\HI\Plants\HI_PlantsVector.gdb\HI_R_Plants_NL48_NADUTMZone4N_missing_20150114_SJ'


outpath_base = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Results\NonL48\GU'+ os.sep + group
outpath_final = outpath_base + os.sep+ region +  group + 'Vector.gdb'

outpath = outpath_base + os.sep + "Working.gdb"

lyrPath = outpath_base + os.sep +'lyr'

VectorLocation = r"C:\Users\Admin\Documents\Jen\Workspace\UseSites\GU_VectorUses.gdb"

flag = region+ "_missing_20160114_" +group

#flag =  fileType + group

exportdict = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\VectorNewLayersDict_export.csv"

errorjoin = int(-88888)
errorzonal = int(-99999)
errorcode = int(-66666)
othercode = int(-77777)
arcpy.CheckOutExtension("Spatial")


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def createdicts(csvfile):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname

def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

CreateGDB(outpath_base,'Working.gdb',outpath)

export_dict = {}

export_dict = createdicts(exportdict)
for k in export_dict:
    value = export_dict[k]

for use in fcs_in_workspace(VectorLocation):
    count = 0
    runID = str(flag) + '_' + str(use)
    # print runID
    usepath = VectorLocation + os.sep + str(use)
    # print dem
    usepath = usepath.replace('\\\\', '\\')
    #print export_dict
    out = export_dict[str(usepath)]

    outFC_use = out + os.sep + runID
    #print outFC_use

    if arcpy.Exists(outFC_use):
        print "Already complete analysis for {0}".format(use)
        continue

    print "Run to " + str(runID)
    arcpy.env.overwriteOutput = True
    start_loop = datetime.datetime.now()

    arcpy.AddField_management(fc, "CoOccur_Acres", "DOUBLE", "#", "#", "#", "#", "NULLABLE", "NON_REQUIRED", "#")
    with arcpy.da.SearchCursor(fc, ("EntityID","CoOccur_Acres")) as clipper:
        for rcrd in clipper:
            if rcrd[1]!= None:
                continue
            else:
                ent = rcrd[0]
                lyr = "Spe_{0}_lyr".format(ent)
                out_layer = lyrPath + os.sep + lyr + ".lyr"
                where = "EntityID = '%s'" % ent
                arcpy.MakeFeatureLayer_management(fc, lyr, where)
                print "Creating layer {0}".format(lyr)
                arcpy.SaveToLayerFile_management(lyr, out_layer, "ABSOLUTE")
                print "Saved layer file"
                env.workspace = outpath
                in_features = usepath
                clip_features = lyr
                out_feature_class = outpath + os.sep + lyr
                xy_tolerance = ""
                print "Clipping"
                arcpy.Clip_analysis(in_features, clip_features, out_feature_class, xy_tolerance)
                arcpy.AddField_management(out_feature_class, "Acres", "DOUBLE", "#", "#", "#", "#", "NULLABLE","NON_REQUIRED", "#")
                print "Calculating Acres"
                arcpy.CalculateField_management(out_feature_class, "Acres", "!shape.area@acres!", "PYTHON_9.3", "#")
                with arcpy.da.SearchCursor(out_feature_class, ("Acres")) as cursor:
                    total_acres = 0
                    for row in cursor:
                        acres = row[0]
                        total_acres = total_acres + acres

                arcpy.AddMessage("Data transfer...")
                with arcpy.da.UpdateCursor(fc, ("EntityID", "CoOccur_Acres")) as cursor:
                    for row in cursor:
                        if row[0] != ent:
                            continue
                        else:
                            row[1] = total_acres

                            cursor.updateRow(row)
                    del row, cursor
    print "Run to " + str(runID)

    outFC = outpath_final + os.sep + runID
    desc = arcpy.Describe(fc)
    filepath = desc.catalogPath
    print outFC
    if not arcpy.Exists(outFC):
        arcpy.Copy_management(filepath, outFC)
        print "Exported: " + str(outFC)

    print outFC_use
    if not arcpy.Exists(outFC_use):
        arcpy.Copy_management(filepath, outFC_use)
        print "Exported: " + str(outFC_use)

    with arcpy.da.UpdateCursor(fc,("CoOccur_Acres")) as cursor:
        for row in cursor:
            if row[0] > -2:
                row[0] = None
                cursor.updateRow(row)
        del row, cursor


    del use
    print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
