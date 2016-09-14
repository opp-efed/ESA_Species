import os
import gc
import time
import datetime
import StreamCrosswalk.functions
import pandas as pd
import arcpy

gc.enable()


# ## to do create streams layers by huc (huc_fc) onces then open it rather than making it for each species
# ##possibly speed up by only extracting information need from table ie reach code, save to a list, append list then
### write to a CSV at end for each rather than using table to table conversion
###add code to export what is "print" to a txt


arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
# removed 286 due to memory issue manuall check


outfile = 'CheckHUCRange_20160519' + '.csv'
fileType = "Range"
outlocation ='C:\Workspace\ESA_Species\StreamCrosswalk'
MasterSpeFC = r"J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\R_WebApp_Composite.gdb"

HUC2Field = "VPU_ID"
HUC2_lwr48 = "C:\Workspace\ESA_Species\StreamCrosswalk\VPU.gdb\VPU_Final"


# Stream files
huc1 = "J:\NHDPlusV2\NHDPlus01\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc2 = "J:\NHDPlusV2\NHDPlus02\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3N = "J:\NHDPlusV2\NHDPlus03N\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3S = "J:\NHDPlusV2\NHDPlus03S\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3W = "J:\NHDPlusV2\NHDPlus3W\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc4 = "J:\NHDPlusV2\NHDPlus04\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc5 = "J:\NHDPlusV2\NHDPlus05\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc6 = "J:\NHDPlusV2\NHDPlus06\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc7 = "J:\NHDPlusV2\NHDPlus07\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc8 = "J:\NHDPlusV2\NHDPlus08\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc9 = "J:\NHDPlusV2\NHDPlus09\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc10L = "J:\NHDPlusV2\NHDPlus10L\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc10U = "J:\NHDPlusV2\NHDPlus10U\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc11 = "J:\NHDPlusV2\NHDPlus11\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc12 = "J:\NHDPlusV2\NHDPlus12\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc13 = "J:\NHDPlusV2\NHDPlus13\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc14 = "J:\NHDPlusV2\NHDPlus14\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc15 = "J:\NHDPlusV2\NHDPlus15\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc16 = "J:\NHDPlusV2\NHDPlus16\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc17 = "J:\NHDPlusV2\NHDPlus17\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc18 = "J:\NHDPlusV2\NHDPlus18\NHDSnapshot\Hydrography\NHDFlowline.shp"


def start_times(startclock):
    start_time = datetime.datetime.fromtimestamp(startclock)
    print "Start Time: " + str(start_time)
    print start_time.ctime()


def end_times(endclock, startclock):
    start_time = datetime.datetime.fromtimestamp(startclock)
    end = datetime.datetime.fromtimestamp(endclock)
    print "End Time: " + str(end)
    print end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


def streamcrosswalk(fc, HUC2_Lowr48, LYR_dir, specieHUCdict):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")

    for row in arcpy.SearchCursor(fc):
        lyr = fileType + "_{0}_lyr".format(row.EntityID)
        out_layer = LYR_dir + os.sep + lyr + ".lyr"
        id = fileType + "_{0}_lyr".format(row.EntityID)
        id = id[:-4]
        id = id.replace((fileType + "_"), "")

        print id
        if not os.path.exists(out_layer):
            whereclause = "EntityID = '%s'" % (id)
            print whereclause
            arcpy.MakeFeatureLayer_management(fc, lyr, whereclause)
            print "Creating layer {0}".format(lyr)
            arcpy.SaveToLayerFile_management(lyr, out_layer, "ABSOLUTE")
            print "Saved layer file"
            del lyr

        spec_location = str(out_layer)
        arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
        arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
        result = int(arcpy.GetCount_management("slt_lyr").getOutput(0))
        print result
        if result is 0:
            print 'NL48'
            specieHUCdict[id] = 'NL48'
        else:
            rows = arcpy.da.SearchCursor("slt_lyr", HUC2Field)
            HUC2set = set()
            HUC2list = []
            for row in rows:
                t = str(row[0])
                HUC2list.append(t)

            HUC2set.update(HUC2list)
            print HUC2set
            HUC2list = list(HUC2set)
            specieHUCdict[id] = HUC2list
            del HUC2list, HUC2set


    arcpy.Delete_management( "slt_lyr")
    arcpy.Delete_management("HUC48_lyr")


datelist = []
today = datetime.date.today()
datelist.append(today)

specieHUCdict = {}
HUCdict = {}
for k, v in globals().items():
    if k.startswith('huc'):
        num = k.replace("huc", "")
        HUCdict[num] = v

start = time.time()
start_times(start)
LYR_dir=r'J:\Workspace\ESA_Species\temp'
createdirectory(LYR_dir)
for fc in StreamCrosswalk.functions.fcs_in_workspace(MasterSpeFC):


    print fc
    print specieHUCdict
    inputfc = MasterSpeFC +os.sep +fc
    streamcrosswalk(inputfc, HUC2_lwr48,LYR_dir,  specieHUCdict)

listHUCs = specieHUCdict.keys()
results = []

for value in listHUCs:
    entid = value
    HUCSs = specieHUCdict[value]
    for huc in HUCSs:
        listresult = [entid, huc, fileType]
        print listresult
        results.append(listresult)

outDF = pd.DataFrame(results)
# print outDF

outpath = outlocation + os.sep + outfile
outDF.to_csv(outpath)

done = time.time()
end_times(done, start)
