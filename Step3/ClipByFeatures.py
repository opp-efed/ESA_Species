import arcpy, traceback, os, sys, csv, time, datetime, functions
from arcpy import env
import functions


SingleFC = False
MutliGDB = True

out_working = 'J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\March2016 composites\ClippedForAcres'
infc_gdb = ['J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\March2016 composites\L48_CH_SpGroup_Composite.gdb',
            'J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\March2016 composites\L48_R_SpGroup_Composite.gdb']
clipfc ='J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\CompositesForClip\Boundaries.gdb\State__territories_WGS_2'




def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

if SingleFC:
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(infc_gdb, "fc_lyr")
    path, fc = os.path.split(infc_gdb)
    clippath, fc_clip = os.path.split(clipfc)
    print "Starting Clip for {0} with ".format(fc, fc_clip)
    outfc = out_working + os.sep + fc + "_LandClip"
    if arcpy.Exists(outfc):
        print "{0} Already clipped".format(outfc)
    else:
        arcpy.env.workspace = out_working
        arcpy.env.overwriteOutput = True
        arcpy.Clip_analysis("fc_lyr", clipfc, outfc)
else:
    if MutliGDB:
        for v in infc_gdb:
            ingdb = v
            for fc in functions.fcs_in_workspace(ingdb ):
                start_loop = datetime.datetime.now()
                path, gdb = os.path.split(ingdb)
                inloopfc =ingdb +os.sep +fc
                arcpy.Delete_management("fc_lyr")
                arcpy.MakeFeatureLayer_management(inloopfc, "fc_lyr")
                clippath, fc_clip = os.path.split(clipfc)
                outgdbpath = out_working+ os.sep+ gdb
                CreateGDB(out_working,gdb,outgdbpath)
                print "Starting Clip for {0} with ".format(fc, fc_clip)
                outfc = outgdbpath + os.sep + fc + "_LandClip"
                if arcpy.Exists(outfc):
                    print "{0} Already clipped".format(outfc)
                else:
                    arcpy.env.workspace = out_working
                    arcpy.env.overwriteOutput = True
                    arcpy.Clip_analysis("fc_lyr", clipfc, outfc)

                print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)
    else:

        for fc in functions.fcs_in_workspace(infc_gdb):
            start_loop = datetime.datetime.now()
            path, gdb = os.path.split(infc_gdb)
            inloopfc =infc_gdb +os.sep +fc
            arcpy.Delete_management("fc_lyr")
            arcpy.MakeFeatureLayer_management(inloopfc, "fc_lyr")
            clippath, fc_clip = os.path.split(clipfc)
            outgdbpath = out_working+ os.sep+ gdb
            CreateGDB(out_working,gdb,outgdbpath)
            print "Starting Clip for {0} with ".format(fc, fc_clip)
            outfc = outgdbpath + os.sep + fc + "_LandClip"
            if arcpy.Exists(outfc):
                print "{0} Already clipped".format(outfc)
            else:
                arcpy.env.workspace = out_working
                arcpy.env.overwriteOutput = True
                arcpy.Clip_analysis("fc_lyr", clipfc, outfc)

            print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
