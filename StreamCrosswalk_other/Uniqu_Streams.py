import os
import gc
import time
import datetime
import StreamCrosswalk.functions
import pandas as pd
import arcpy

gc.enable()
File_dir = r'J:\Workspace\ESA_Species\StreamCrosswalk\UniqueWaterbodies'

# ## to do create streams layers by huc (huc_fc) onces then open it rather than making it for each species
# ##possibly speed up by only extracting information need from table ie reach code, save to a list, append list then
### write to a CSV at end for each rather than using table to table conversion
###add code to export what is "print" to a txt


arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
# removed 286 due to memory issue manuall check



outlocation = 'J:\Workspace\ESA_Species\StreamCrosswalk\UniqueWaterbodies'
MasterSpeFC = r"J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\L48_R_SpGroup_Composite_1.gdb"

HUC2Field = "VPU_ID"
HUC2_lwr48 = "C:\Workspace\ESA_Species\StreamCrosswalk\VPU.gdb\VPU_Final"


# Stream files
#huc1 = "J:\NHDPlusV2\NHDPlus01\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc2 = "J:\NHDPlusV2\NHDPlus02\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc3N = "J:\NHDPlusV2\NHDPlus03N\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc3S = "J:\NHDPlusV2\NHDPlus03S\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc3W = "J:\NHDPlusV2\NHDPlus03W\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc4 = "J:\NHDPlusV2\NHDPlus04\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc5 = "J:\NHDPlusV2\NHDPlus05\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc6 = "J:\NHDPlusV2\NHDPlus06\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc7 = "J:\NHDPlusV2\NHDPlus07\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc8 = "J:\NHDPlusV2\NHDPlus08\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc9 = "J:\NHDPlusV2\NHDPlus09\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc10L = "J:\NHDPlusV2\NHDPlus10L\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc10U = "J:\NHDPlusV2\NHDPlus10U\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc11 = "J:\NHDPlusV2\NHDPlus11\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc12 = "J:\NHDPlusV2\NHDPlus12\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc13 = "J:\NHDPlusV2\NHDPlus13\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc14 = "J:\NHDPlusV2\NHDPlus14\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc15 = "J:\NHDPlusV2\NHDPlus15\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc16 = "J:\NHDPlusV2\NHDPlus16\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc17 = "J:\NHDPlusV2\NHDPlus17\NHDSnapshot\Hydrography\NHDFlowline.shp"
#huc18 = "J:\NHDPlusV2\NHDPlus18\NHDSnapshot\Hydrography\NHDFlowline.shp"

huc1 = "J:\NHDPlusV2\NHDPlus01\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc2 = "J:\NHDPlusV2\NHDPlus02\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc3N = "J:\NHDPlusV2\NHDPlus03N\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc3S = "J:\NHDPlusV2\NHDPlus03S\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc3W = "J:\NHDPlusV2\NHDPlus03W\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc4 = "J:\NHDPlusV2\NHDPlus04\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc5 = "J:\NHDPlusV2\NHDPlus05\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc6 = "J:\NHDPlusV2\NHDPlus06\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc7 = "J:\NHDPlusV2\NHDPlus07\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc8 = "J:\NHDPlusV2\NHDPlus08\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc9 = "J:\NHDPlusV2\NHDPlus09\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc10L = "J:\NHDPlusV2\NHDPlus10L\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc10U = "J:\NHDPlusV2\NHDPlus10U\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc11 = "J:\NHDPlusV2\NHDPlus11\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc12 = "J:\NHDPlusV2\NHDPlus12\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc13 = "J:\NHDPlusV2\NHDPlus13\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc14 = "J:\NHDPlusV2\NHDPlus14\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc15 = "J:\NHDPlusV2\NHDPlus15\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc16 = "J:\NHDPlusV2\NHDPlus16\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc17 = "J:\NHDPlusV2\NHDPlus17\NHDSnapshot\Hydrography\NHDWaterbody.shp"
huc18 = "J:\NHDPlusV2\NHDPlus18\NHDSnapshot\Hydrography\NHDWaterbody.shp"


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


def streamcrosswalk(fc, HUC2_Lowr48, LYR_dir, DBF_dir, specieHUCdict,HUCdict):
    path, filename = os.path.split(fc)
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
    arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", "fc_lyr", "#", "ADD_TO_SELECTION")
    arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
    result = int(arcpy.GetCount_management("slt_lyr").getOutput(0))
    print result
    if result is 0:
        print 'NL48'
        specieHUCdict[id] = 'NL48'
    else:
        rows = arcpy.da.SearchCursor("slt_lyr", HUC2Field)
        HUC2list = []
        for row in rows:
            t = str(row[0])
            if t not in HUC2list:
                HUC2list.append(t)

    arcpy.SelectLayerByAttribute_management("HUC48_lyr", "CLEAR_SELECTION")
    counter = 0
    print HUC2list
    #print HUCdict

    for z in HUC2list:
        huc_fc = HUCdict[z]
        if huc_fc is not None:
            arcpy.Delete_management("huc_lyr")
            arcpy.MakeFeatureLayer_management(huc_fc, "huc_lyr")
            lyr2 = "Stream_" + str(counter)+"_"+ str(filename)+"_"+ str(z)+'.lyr'
            print "\n{0}".format(str(lyr2))
            out_layer2 = os.path.join(LYR_dir, lyr2)
            print out_layer2
            arcpy.SaveToLayerFile_management("huc_lyr", out_layer2, "ABSOLUTE")
            # print "saved layer"
            arcpy.SelectLayerByLocation_management("huc_lyr", "INTERSECT", "fc_lyr", "", "NEW_SELECTION")
            count = arcpy.GetCount_management("huc_lyr")
            print str(count) + " selected features"
            tableview = "tbl_view_" + str(counter)
            arcpy.MakeTableView_management("huc_lyr", tableview)
            count = int(arcpy.GetCount_management(tableview).getOutput(0))
            print str(count) + " Tableview"

            outtable = File_dir+os.sep+str(filename) +'.dbf'

            if count < 1:
                    continue
            else:

                if not arcpy.Exists (outtable):
                    arcpy.TableToTable_conversion(tableview, File_dir, (str(filename) +'.dbf'))
                    print "created table: " + str(outtable)
                    counter = counter + 1
                else:
                    tableview = "tbl_view_" + str(counter)
                    arcpy.Delete_management("outable")
                    arcpy.MakeTableView_management(outtable, "outable")
                    arcpy.Append_management(tableview, "outable", "NO_TEST", "", "")
                    print "table {0} appended. Located at {1}".format(outtable, File_dir)
                    counter = counter + 1
    print 'completed {0} out table at {1}'.format(fc, outtable)
    del row,   HUC2list

    arcpy.Delete_management("slt_lyr")
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
LYR_dir = File_dir + os.sep + 'LYR'
DBF_dir = File_dir + os.sep + 'DBF'
createdirectory(LYR_dir)
createdirectory(DBF_dir)


for fc in StreamCrosswalk.functions.fcs_in_workspace(MasterSpeFC):
    print fc
    print specieHUCdict
    inputfc = MasterSpeFC + os.sep + fc
    streamcrosswalk(inputfc, HUC2_lwr48, LYR_dir, DBF_dir, specieHUCdict, HUCdict)

done = time.time()
end_times(done, start)
