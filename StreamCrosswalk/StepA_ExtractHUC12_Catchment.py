import os
import gc
import time
import datetime
import csv

import arcpy

gc.enable()

# NOTE NOTE if the script is stop and an species that was started but not completed for a species it
# must be deleted before starting the script again.If a table has been created the script will
# move to the next species

arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
extractfiles = 'catchment'
ws = r"L:\Workspace\ESA_Species\Range\HUC12\NMFS" + os.sep + extractfiles
name_dir = "GDB"
out_location = ws + os.sep + name_dir

###folder used as temp work space

noNHDCSV = extractfiles + "_noNHD_20160907"

# GDB with all Composites to Run
MasterSpeFC = r"L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_SpGroupComposite.gdb"
# MasterSpeFC = r"C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite.gdb"

HUC2Field = "VPU_ID"
HUC2_lwr48 = "C:\WorkSpace\FinalBE_EucDis_CoOccur\Boundaries.gdb\VPU_Final"

FWSaqu_species = ['153',
'154',
'155',
'160',
'286',
'1509',
'1769',
'2448',
'2510',
'2514',
'2528',
'2842',
'2891',
'3096',
'3133',
'3199',
'3318',
'3398',
'3654',
'4093',
'4112',
'4274',
'4300',
'4330',
'4719',
'4799',
'4881',
'4992',
'5180',
'5265',
'5623',
'5658',
'5815',
'5989',
'6220',
'6578',
'6843',
'6966',
'7115',
'7590',
'7834',
'7855',
'7989',
'8181',
'8241',
'8278',
'8462',
'9021',
'9126',
'9382',
'9384',
'9432',
'9707',
'9941',
'10013',
'10077',
'10142',
'10144',
'10145',
'10150',
'10151',
'10153',
'10297',
'10298',
'10299',
'10300',
'10301',
'10310',
'10311',
'10312',
'10314',
'10319',
'10323',
'10326',
'10332',
'10340',
'10341',
'10370',
'10381',
'10485',
'10700',
'10733',
'10734',
'10736',
'10903',
'10908',
'11175',
'11176',
'11191',
'11192',
'11193',
'NMFS125',
'NMFS134',
'NMFS137',
'NMFS138',
'NMFS139',
'NMFS159',
'NMFS166',
'NMFS173',
'NMFS174',
'NMFS175',
'NMFS176',
'NMFS177',
'NMFS178',
'NMFS179',
'NMFS180',
'NMFS181',


]


# NHD files
# NOTE UPDATE THESE PATHS IF YOU WANT TO EXTRACT CATCHMENT
#
# huc1 = "L:\NHDPlusV2\NHDPlus01\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc2 = "L:\NHDPlusV2\NHDPlus02\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc3N = "L:\NHDPlusV2\NHDPlus03N\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc3S = "L:\NHDPlusV2\NHDPlus03S\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc3W = "L:\NHDPlusV2\NHDPlus03W\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc4 = "L:\NHDPlusV2\NHDPlus04\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc5 = "L:\NHDPlusV2\NHDPlus05\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc6 = "L:\NHDPlusV2\NHDPlus06\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc7 = "L:\NHDPlusV2\NHDPlus07\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc8 = "L:\NHDPlusV2\NHDPlus08\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc9 = "L:\NHDPlusV2\NHDPlus09\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc10L = "L:\NHDPlusV2\NHDPlus10L\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc10U = "L:\NHDPlusV2\NHDPlus10U\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc11 = "L:\NHDPlusV2\NHDPlus11\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc12 = "L:\NHDPlusV2\NHDPlus12\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc13 = "L:\NHDPlusV2\NHDPlus13\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc14 = "L:\NHDPlusV2\NHDPlus14\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc15 = "L:\NHDPlusV2\NHDPlus15\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc16 = "L:\NHDPlusV2\NHDPlus16\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc17 = "L:\NHDPlusV2\NHDPlus17\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc18 = "L:\NHDPlusV2\NHDPlus18\WBDSnapshot\WBD\WBD_Subwatershed.shp"

#
huc1 = "L:\NHDPlusV2\NHDPlus01\NHDPlusCatchment\Catchment.shp"
huc2 = "L:\NHDPlusV2\NHDPlus02\NHDPlusCatchment\Catchment.shp"
huc3N = "L:\NHDPlusV2\NHDPlus03N\NHDPlusCatchment\Catchment.shp"
huc3S = "L:\NHDPlusV2\NHDPlus03S\NHDPlusCatchment\Catchment.shp"
huc3W = "L:\NHDPlusV2\NHDPlus03W\NHDPlusCatchment\Catchment.shp"
huc4 = "L:\NHDPlusV2\NHDPlus04\NHDPlusCatchment\Catchment.shp"
huc5 = "L:\NHDPlusV2\NHDPlus05\NHDPlusCatchment\Catchment.shp"
huc6 = "L:\NHDPlusV2\NHDPlus06\NHDPlusCatchment\Catchment.shp"
huc7 = "L:\NHDPlusV2\NHDPlus07\NHDPlusCatchment\Catchment.shp"
huc8 = "L:\NHDPlusV2\NHDPlus08\NHDPlusCatchment\Catchment.shp"
huc9 = "L:\NHDPlusV2\NHDPlus09\NHDPlusCatchment\Catchment.shp"
huc10L = "L:\NHDPlusV2\NHDPlus10L\NHDPlusCatchment\Catchment.shp"
huc10U = "L:\NHDPlusV2\NHDPlus10U\NHDPlusCatchment\Catchment.shp"
huc11 = "L:\NHDPlusV2\NHDPlus11\NHDPlusCatchment\Catchment.shp"
huc12 = "L:\NHDPlusV2\NHDPlus12\NHDPlusCatchment\Catchment.shp"
huc13 = "L:\NHDPlusV2\NHDPlus13\NHDPlusCatchment\Catchment.shp"
huc14 = "L:\NHDPlusV2\NHDPlus14\NHDPlusCatchment\Catchment.shp"
huc15 = "L:\NHDPlusV2\NHDPlus15\NHDPlusCatchment\Catchment.shp"
huc16 = "L:\NHDPlusV2\NHDPlus16\NHDPlusCatchment\Catchment.shp"
huc17 = "L:\NHDPlusV2\NHDPlus17\NHDPlusCatchment\Catchment.shp"
huc18 = "L:\NHDPlusV2\NHDPlus18\NHDPlusCatchment\Catchment.shp"


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


def streamcrosswalk(fc, HUC2_Lowr48, LYR_dir, outlocation, HUCdict, NoNHD, csvpath, spec_to_include, name_dir):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    for row in arcpy.SearchCursor(fc):
        filename = row.filename
        entid = row.EntityID
        if entid not in spec_to_include:
            continue

        sp_group = fc.split("_")[1]

        sp_gdb = sp_group + '_' + extractfiles + '.gdb'
        out_gdb = out_location + os.sep + sp_gdb
        CreateGDB(outlocation, sp_gdb, out_gdb)

        filename_new = filename.replace('_catchment', '')
        filename_new = filename_new + "_" + extractfiles

        outfc = out_gdb + os.sep + filename_new
        out_layer = LYR_dir + os.sep + entid + ".lyr"

        # if not os.path.exists(out_layer):
        if not arcpy.Exists(outfc):
            whereclause = "EntityID = '%s'" % (entid)
            print whereclause
            arcpy.MakeFeatureLayer_management(fc, "lyr", whereclause)
            print "Creating layer {0}".format(entid)
            arcpy.SaveToLayerFile_management("lyr", out_layer, "ABSOLUTE")
            print "Saved layer file"

            spec_location = str(out_layer)
            arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
            arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
            with arcpy.da.SearchCursor("slt_lyr", HUC2Field) as cursor:
                HUC2list = sorted({row[0] for row in cursor})
                print HUC2list

            # for each value in the HUC2 set will select all HUC12 that are with species file, and save to a master
            # species HUC12 fc, one table per species will include all values

            # NOTE NOTE if the script is stop and an species that was started but not completed for a species it
            # must be deleted before starting the script again.If a table has been created the script will
            # move to the next species
            counter = 0
            for z in HUC2list:
                print z
                huc_fc = HUCdict.get(z)
                print huc_fc
                if huc_fc is not None:
                    arcpy.Delete_management("huc_lyr")
                    arcpy.MakeFeatureLayer_management(huc_fc, "huc_lyr")
                    arcpy.SelectLayerByLocation_management("huc_lyr", "INTERSECT", out_layer, "", "NEW_SELECTION")
                    count = arcpy.GetCount_management("huc_lyr")
                    print str(count) + " selected features"

                    if count < 1:
                        print 'Zero'
                        if entid not in noNHD:
                            NoNHD.append(entid)
                        continue
                    if counter == 0:
                        if count != 0:
                            print outfc
                            arcpy.CopyFeatures_management("huc_lyr", outfc)
                            print "exported: " + str(outfc)
                            counter += 1
                            if entid in noNHD:
                                NoNHD.remove(id)
                        continue
                    if counter > 0:
                        arcpy.Append_management("huc_lyr", outfc, "NO_TEST", "", "")
                        counter += 1
            print "FC {0} completed. Located at {1}".format(outfc, out_gdb)
            del row, HUC2list
        else:
            print "{0}".format(outfc) + " previously populated"
            continue

    arcpy.Delete_management("HUC48_lyr")
    create_outtable(NoNHD, csvpath)
    return NoNHD


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

csvfile, csvpath = create_csvflnm_timestamp(noNHDCSV, DBF_dir)
noNHD = []

createdirectory(File_dir)
# createdirectory(GDB_dir)
createdirectory(LYR_dir)
createdirectory(DBF_dir)

arcpy.env.workspace = MasterSpeFC
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    streamcrosswalk(fc, HUC2_lwr48, LYR_dir, out_location, HUCdict, noNHD, csvpath, FWSaqu_species, name_dir)

print noNHD
done = time.time()
end_times(done, start)
