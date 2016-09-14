import os
import gc
import time
import datetime
import csv

import arcpy

gc.enable()


# ## to do create streams layers by huc (huc_fc) onces then open it rather than making it for each species
# ##possibly speed up by only extracting information need from table ie reach code, save to a list, append list then
### write to a CSV at end for each rather than using table to table conversion
###add code to export what is "print" to a txt


arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"

ws = r"J:\Workspace\ESA_Species\Range\NAD83"
name_dir = "RawAquatics"
outGDB = "Crustaceans_catchment"
OUTGDBpath =r'J:\Workspace\ESA_Species\Range\NAD83\RawAquatics\Crustaceans_catchment.gdb'

HUC2Field = "VPU_ID"
###folder used as temp work space

noNHDCSV = "ToposCatchment_noNHD"
MasterSpeFC = r"J:\Workspace\ESA_Species\Range\NAD83\RawAquatics\raw_crustaceans.gdb\R_478_poly_20150520_STD_NAD83"
##TODO add option to look through indvidual ifle esin addtion to Comp
HUC2_lwr48 = r"J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Range\Fishes\Range_Catchments\VPU.gdb\VPU_Final"

#Stream files
huc1 = "J:\NHDPlusV2\NHDPlus01\NHDPlusCatchment\Catchment.shp"
huc2 = "J:\NHDPlusV2\NHDPlus02\NHDPlusCatchment\Catchment.shp"
huc3N = "J:\NHDPlusV2\NHDPlus03N\NHDPlusCatchment\Catchment.shp"
huc3S = "J:\NHDPlusV2\NHDPlus03S\NHDPlusCatchment\Catchment.shp"
huc3W = "J:\NHDPlusV2\NHDPlus03W\NHDPlusCatchment\Catchment.shp"
huc4 = "J:\NHDPlusV2\NHDPlus04\NHDPlusCatchment\Catchment.shp"
huc5 = "J:\NHDPlusV2\NHDPlus05\NHDPlusCatchment\Catchment.shp"
huc6 = "J:\NHDPlusV2\NHDPlus06\NHDPlusCatchment\Catchment.shp"
huc7 = "J:\NHDPlusV2\NHDPlus07\NHDPlusCatchment\Catchment.shp"
huc8 = "J:\NHDPlusV2\NHDPlus08\NHDPlusCatchment\Catchment.shp"
huc9 = "J:\NHDPlusV2\NHDPlus09\NHDPlusCatchment\Catchment.shp"
huc10L = "J:\NHDPlusV2\NHDPlus10L\NHDPlusCatchment\Catchment.shp"
huc10U = "J:\NHDPlusV2\NHDPlus10U\NHDPlusCatchment\Catchment.shp"
huc11 = "J:\NHDPlusV2\NHDPlus11\NHDPlusCatchment\Catchment.shp"
huc12 = "J:\NHDPlusV2\NHDPlus12\NHDPlusCatchment\Catchment.shp"
huc13 = "J:\NHDPlusV2\NHDPlus13\NHDPlusCatchment\Catchment.shp"
huc14 = "J:\NHDPlusV2\NHDPlus14\NHDPlusCatchment\Catchment.shp"
huc15 = "J:\NHDPlusV2\NHDPlus15\NHDPlusCatchment\Catchment.shp"
huc16 = "J:\NHDPlusV2\NHDPlus16\NHDPlusCatchment\Catchment.shp"
huc17 = "J:\NHDPlusV2\NHDPlus17\NHDPlusCatchment\Catchment.shp"
huc18 = "J:\NHDPlusV2\NHDPlus18\NHDPlusCatchment\Catchment.shp"


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


def streamcrosswalk(fc, HUC2_Lowr48, LYR_dir, DBF_dir, outgdbpath, HUCdict, NoNHD,csvpath, outpath):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    for row in arcpy.SearchCursor(fc):

        id = row.EntityID
        print id
        outfc= outpath + os.sep + "R_"+id+"_catchment"
        out_layer = LYR_dir + os.sep + id + ".lyr"

        #if not os.path.exists(out_layer):
        if not arcpy.Exists(outfc):
            whereclause = "EntityID = '%s'" % (id)
            print whereclause
            arcpy.MakeFeatureLayer_management(fc, "lyr", whereclause)
            print "Creating layer {0}".format(id)
            arcpy.SaveToLayerFile_management("lyr", out_layer, "ABSOLUTE")
            print "Saved layer file"

            try:
                spec_location = str(out_layer)
                arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
                arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
                rows = arcpy.da.SearchCursor("slt_lyr", HUC2Field)
                HUC2set = set()
                HUC2list = []
                for row in rows:
                    t = str(row[0])
                    HUC2list.append(t)
            except:
                print "Failed Selection for {0}".format(fc)
            HUC2set.update(HUC2list)
            #print "set for {0}".format(lyr)
            print HUC2set
            counter = 0
            for z in HUC2set:
                print z
                huc_fc = HUCdict.get(z)
                print huc_fc
                if huc_fc is not None:
                    arcpy.Delete_management("huc_lyr")
                    arcpy.MakeFeatureLayer_management(huc_fc, "huc_lyr")
                    lyr2 = "Stream_" + str(counter) + str(id)
                    out_layer2 = os.path.join(LYR_dir, lyr2)
                    print out_layer2
                    arcpy.SaveToLayerFile_management("huc_lyr", out_layer2, "ABSOLUTE")
                    arcpy.SelectLayerByLocation_management("huc_lyr", "INTERSECT", out_layer, "", "NEW_SELECTION")
                    count = arcpy.GetCount_management("huc_lyr")
                    print str(count) + " selected features"

                    if count < 1:
                        print 'Zero'
                        filename = str(fc)
                        if id not in noNHD:
                            noNHD.append(id)
                        continue
                    if counter == 0:
                        if count != 0:
                            print outfc
                            arcpy.CopyFeatures_management("huc_lyr", outfc)
                            print "exported: " + str(outfc)
                            counter = counter + 1
                            if id in noNHD:
                                noNHD.remove(id)
                        continue
                    if counter > 0:
                        arcpy.Append_management("huc_lyr", outfc, "NO_TEST", "", "")
                        counter = counter + 1
            print "FC {0} completed. Located at {1}".format(outfc, outpath)
            del row, HUC2set, HUC2list
        else:
            print "{0}".format(outfc) + " previously populated"

    arcpy.Delete_management("HUC48_lyr")
    create_outtable(noNHD, csvpath)
    return noNHD


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


def create_gdbflnm_timestamp(namegdbfile, outgdblocation):
    filename = str(namegdbfile) + "_" + str(datelist[0])
    filepath = os.path.join(outgdblocation, (filename + '.gdb'))
    return filename, filepath


def create_outtable(listname, csvlocation):
    with open(csvlocation, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in listname:
            writer.writerow([val])


def create_csvflnm_timestamp(NameCSVFile, outCSVLocation):
    filename = str(NameCSVFile) + "_" + str(datelist[0]) + '.csv'
    filepath = os.path.join(outCSVLocation, filename)
    return (filename, filepath)


datelist = []
today = datetime.date.today()
datelist.append(today)
File_dir = ws + os.sep + str(name_dir)
GDB_dir = File_dir + os.sep + 'GDB'
LYR_dir = File_dir + os.sep + 'LYR'
DBF_dir = File_dir + os.sep + 'DBF'

HUCdict = {}
for k, v in globals().items():
    if k.startswith('huc'):
        num = k.replace("huc", "")
        HUCdict[num] = v

start = time.time()
start_times(start)

OutGDB, outgdbpath = create_gdbflnm_timestamp(outGDB, File_dir)
csvfile, csvpath = create_csvflnm_timestamp(noNHDCSV, DBF_dir)
noNHD = []

createdirectory(File_dir)
#createdirectory(GDB_dir)
createdirectory(LYR_dir)
createdirectory(DBF_dir)
#CreateGDB(GDB_dir, OutGDB, outgdbpath)
outgdbpath = OUTGDBpath
streamcrosswalk(MasterSpeFC, HUC2_lwr48, LYR_dir, DBF_dir, outgdbpath, HUCdict, noNHD,csvpath,outgdbpath)
print noNHD
done = time.time()
end_times(done, start)
