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
ws = r"J:\Workspace\ESA_Species\CriticalHabitat" + os.sep + extractfiles
name_dir = "GDB"
out_location = ws + os.sep + name_dir

###folder used as temp work space

noNHDCSV = extractfiles + "_noNHD_20160907"

# GDB with all Composites to Run
MasterSpeFC = r"C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\zCriticalHabitat\CH_SpGroupComposite.gdb"
# MasterSpeFC = r"C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite.gdb"

HUC2Field = "VPU_ID"
HUC2_lwr48 = "C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Boundaries.gdb\VPU_Final"

FWSaqu_species = ['7', '11262', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218',
                  '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232',
                  '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246',
                  '247', '248', '249', '250', '251', '252', '253', '254', '255', '256', '257', '258', '259', '260',
                  '261', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272', '273', '274',
                  '275', '276', '277', '278', '279', '280', '281', '282', '283', '284', '285', '287', '288', '290',
                  '291', '292', '293', '294', '295', '296', '297', '298', '299', '300', '301', '303', '305', '306',
                  '307', '308', '309', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321',
                  '322', '323', '324', '325', '326', '327', '328', '329', '330', '331', '332', '333', '334', '335',
                  '336', '337', '338', '339', '340', '341', '342', '343', '344', '345', '346', '347', '348', '349',
                  '350', '351', '352', '353', '354', '355', '356', '357', '358', '359', '360', '361', '392', '396',
                  '402', '407', '411', '414', '415', '416', '417', '441', '468', '478', '479', '362', '363', '364',
                  '365', '366', '367', '368', '369', '370', '371', '372', '373', '374', '375', '2561', '376', '377',
                  '378', '379', '380', '381', '3364', '382', '383', '384', '385', '386', '1369', '1559', '3842', '1680',
                  '1897', '1905', '1934', '2142', '2192', '2308', '2316', '2599', '2917', '2956', '3226', '3280',
                  '3497', '3525',
                  '5065', '3596', '3645', '3833', '3879', '4042', '4086', '4210', '4248', '4411', '4431', '4490',
                  '4496', '5281',
                  '5715', '5718', '5719', '5833', '5856', '5981', '6062', '6223', '6297', '6503', '6534', '6557',
                  '6662', '6841',
                  '7091', '7150', '7177', '7332', '7349', '7847', '7363', '7372', '7512', '7670', '7816', '7949',
                  '8349', '8434',
                  '8356', '8389', '8442', '8561', '8921', '9061', '9220', '9487', '9488', '9489', '9490', '9491',
                  '9492', '9493',
                  '9494', '9495', '9496', '9497', '9498', '9499', '9500', '9501', '9502', '9503', '9504', '9505',
                  '9507', '9506',
                  '9967', '9968', '9969', '10037', '10038', '10039', '10052', '10060', '10910']


# NHD files
# NOTE UPDATE THESE PATHS IF YOU WANT TO EXTRACT CATCHMENT
#
# huc1 = "J:\NHDPlusV2\NHDPlus01\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc2 = "J:\NHDPlusV2\NHDPlus02\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc3N = "J:\NHDPlusV2\NHDPlus03N\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc3S = "J:\NHDPlusV2\NHDPlus03S\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc3W = "J:\NHDPlusV2\NHDPlus03W\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc4 = "J:\NHDPlusV2\NHDPlus04\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc5 = "J:\NHDPlusV2\NHDPlus05\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc6 = "J:\NHDPlusV2\NHDPlus06\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc7 = "J:\NHDPlusV2\NHDPlus07\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc8 = "J:\NHDPlusV2\NHDPlus08\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc9 = "J:\NHDPlusV2\NHDPlus09\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc10L = "J:\NHDPlusV2\NHDPlus10L\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc10U = "J:\NHDPlusV2\NHDPlus10U\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc11 = "J:\NHDPlusV2\NHDPlus11\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc12 = "J:\NHDPlusV2\NHDPlus12\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc13 = "J:\NHDPlusV2\NHDPlus13\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc14 = "J:\NHDPlusV2\NHDPlus14\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc15 = "J:\NHDPlusV2\NHDPlus15\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc16 = "J:\NHDPlusV2\NHDPlus16\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc17 = "J:\NHDPlusV2\NHDPlus17\WBDSnapshot\WBD\WBD_Subwatershed.shp"
# huc18 = "J:\NHDPlusV2\NHDPlus18\WBDSnapshot\WBD\WBD_Subwatershed.shp"


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
