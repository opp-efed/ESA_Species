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
extractfiles = 'HUC12'
ws = r"L:\Workspace\ESA_Species\Range\HUC12\AquWoes_FWS_NMFS" + os.sep + extractfiles
name_dir = "GDB"
out_location = ws + os.sep + name_dir

###folder used as temp work space

noNHDCSV = extractfiles + "_noNHD_20161025"

# GDB with all Composites to Run
MasterSpeFC = r"L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_SpGroupComposite.gdb"
# MasterSpeFC = r"C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite.gdb"

HUC2Field = "VPU_ID"
HUC2_lwr48 = "L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Boundaries.gdb\VPU_Final"

FWSaqu_species = [
    '3280', '312', '292', '291', '210', '287', '9220', '300', '276', '288', '272', '266', '237', '1934', '9061', '9505',
    '246', '9504', '261', '225', '262', '253', '209', '255', '249', '6297', '8561', '254', '263', '3497', '226', '256',
    '309', '10052', '282', '280', '281', '296', '211', '8442', '278', '7670', '277', '299', '242', '3596', '279', '311',
    '10910', '295', '234', '2599', '215', '2142', '268', '264', '265', '267', '227', '273', '290', '5265', '6578',
    '6966', '9021', '6843', '8278', '2448', '2528', '2842', '3654', '4112', '4274', '5658', '5815', '6220', '7989',
    '9432', '2514', '3398', '4300', '4799', '4992', '7590', '7834', '7855', '8241', '248', '220', '222', '233', '223',
    '221', '1509', '5180', '10077', '301', '10037', '4330', '4093', '286', '10297', '10298', '10299', '10300', '10301',
    '314', '303', '252', '243', '285', '283', '284', '251', '217', '216', '275', '274', '231', '218', '8389', '213',
    '250', '214', '230', '219', '232', '6557', '7332', '239', '316', '313', '3879', '315', '228', '6662', '257', '229',
    '224', '308', '6503', '9502', '3525', '244', '269', '212', '307', '10060', '5719', '297', '8921', '306', '293',
    '298', '4431', '294', '238', '240', '235', '259', '258', '5981', '7150', '247', '2956', '4496', '9506', '270',
    '271', '9503', '245', '305', '6416', '10150', '260', '236', '4881', '241', '4248', '453', '2767', '454', '441',
    '439', '484', '1261', '8172', '6596', '477', '475', '5714', '490', '491', '493', '495', '492', '489', '488', '478',
    '479', '487', '480', '482', '481', '476', '486', '483', '494', '355', '363', '354', '375', '343', '4490', '368',
    '9494', '334', '2192', '9493', '386', '4210', '351', '366', '353', '5715', '9491', '358', '1905', '9497', '333',
    '322', '2316', '346', '367', '323', '8349', '324', '365', '348', '319', '374', '320', '6223', '5281', '321', '5856',
    '6534', '339', '5833', '9500', '337', '3226', '9495', '7177', '7363', '7349', '359', '2308', '9489', '331', '372',
    '10038', '325', '357', '369', '4086', '360', '373', '326', '1680', '370', '332', '8356', '9488', '345', '364',
    '4411', '380', '381', '384', '385', '341', '9498', '335', '336', '9501', '340', '9496', '7816', '352', '1897',
    '361', '347', '378', '382', '383', '376', '3833', '349', '377', '338', '9499', '371', '1369', '350', '6841', '2917',
    '342', '356', '379', '7949', '1559', '10039', '3645', '344', '328', '7091', '9969', '330', '5718', '9492', '9968',
    '329', '9487', '362', '327', '9967', '4042', '6062', '318', '317', '7512', '9490', '417', '412', '407', '406',
    '1247', '413', '4437', '402', '1380', '404', '4162', '408', '401', '4479', '6739', '4766', '398', '403', '6138',
    '5362', '418', '409', '399', '1358', '396', '3842', '9507', '411', '8434', '416', '2561', '415', '414', '3364',
    '400', '9382', '10332', '10319', '9384', '10340', '10341', '10310', '10323', '10903', '10314', '10311', '10312',
    '10908', '10370', '10326', '10013', '8462', '10142', '10151', '10153', '2381', '5089', '2929', '5449', '445',
    '1361', '2144', '4326', '6231', '4910', '188', '189', '190', '194', '195', '196', '197', '199', '201', '202', '203',
    '204', '205', '206', '207', '208', '1707', '1740', '3628', '4090', '4773', '5065', '5434', '6346', '7482', '7610',
    '7847', '8231', '8395', '8765', '9694', '9943', '10517', '517', '522', '527', '534', '580', '582', '583', '625',
    '677', '678', '711', '785', '786', '787', '807', '823', '858', '870', '875', '931', '1028', '1047', '1064', '1199',
    '1203', '1204', '7992', '442', '435', '485', '1849', '1953', '6867', '10130', '1245', '1246', '3748', '5386',
    '5666',

]

# NHD files
# NOTE UPDATE THESE PATHS IF YOU WANT TO EXTRACT CATCHMENT
#
huc1 = "L:\NHDPlusV2\NHDPlus01\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc2 = "L:\NHDPlusV2\NHDPlus02\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc3N = "L:\NHDPlusV2\NHDPlus03N\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc3S = "L:\NHDPlusV2\NHDPlus03S\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc3W = "L:\NHDPlusV2\NHDPlus03W\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc4 = "L:\NHDPlusV2\NHDPlus04\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc5 = "L:\NHDPlusV2\NHDPlus05\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc6 = "L:\NHDPlusV2\NHDPlus06\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc7 = "L:\NHDPlusV2\NHDPlus07\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc8 = "L:\NHDPlusV2\NHDPlus08\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc9 = "L:\NHDPlusV2\NHDPlus09\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc10L = "L:\NHDPlusV2\NHDPlus10L\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc10U = "L:\NHDPlusV2\NHDPlus10U\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc11 = "L:\NHDPlusV2\NHDPlus11\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc12 = "L:\NHDPlusV2\NHDPlus12\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc13 = "L:\NHDPlusV2\NHDPlus13\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc14 = "L:\NHDPlusV2\NHDPlus14\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc15 = "L:\NHDPlusV2\NHDPlus15\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc16 = "L:\NHDPlusV2\NHDPlus16\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc17 = "L:\NHDPlusV2\NHDPlus17\WBDSnapshot\WBD\WBD_Subwatershed.shp"
huc18 = "L:\NHDPlusV2\NHDPlus18\WBDSnapshot\WBD\WBD_Subwatershed.shp"


#
# huc1 = "L:\NHDPlusV2\NHDPlus01\NHDPlusCatchment\Catchment.shp"
# huc2 = "L:\NHDPlusV2\NHDPlus02\NHDPlusCatchment\Catchment.shp"
# huc3N = "L:\NHDPlusV2\NHDPlus03N\NHDPlusCatchment\Catchment.shp"
# huc3S = "L:\NHDPlusV2\NHDPlus03S\NHDPlusCatchment\Catchment.shp"
# huc3W = "L:\NHDPlusV2\NHDPlus03W\NHDPlusCatchment\Catchment.shp"
# huc4 = "L:\NHDPlusV2\NHDPlus04\NHDPlusCatchment\Catchment.shp"
# huc5 = "L:\NHDPlusV2\NHDPlus05\NHDPlusCatchment\Catchment.shp"
# huc6 = "L:\NHDPlusV2\NHDPlus06\NHDPlusCatchment\Catchment.shp"
# huc7 = "L:\NHDPlusV2\NHDPlus07\NHDPlusCatchment\Catchment.shp"
# huc8 = "L:\NHDPlusV2\NHDPlus08\NHDPlusCatchment\Catchment.shp"
# huc9 = "L:\NHDPlusV2\NHDPlus09\NHDPlusCatchment\Catchment.shp"
# huc10L = "L:\NHDPlusV2\NHDPlus10L\NHDPlusCatchment\Catchment.shp"
# huc10U = "L:\NHDPlusV2\NHDPlus10U\NHDPlusCatchment\Catchment.shp"
# huc11 = "L:\NHDPlusV2\NHDPlus11\NHDPlusCatchment\Catchment.shp"
# huc12 = "L:\NHDPlusV2\NHDPlus12\NHDPlusCatchment\Catchment.shp"
# huc13 = "L:\NHDPlusV2\NHDPlus13\NHDPlusCatchment\Catchment.shp"
# huc14 = "L:\NHDPlusV2\NHDPlus14\NHDPlusCatchment\Catchment.shp"
# huc15 = "L:\NHDPlusV2\NHDPlus15\NHDPlusCatchment\Catchment.shp"
# huc16 = "L:\NHDPlusV2\NHDPlus16\NHDPlusCatchment\Catchment.shp"
# huc17 = "L:\NHDPlusV2\NHDPlus17\NHDPlusCatchment\Catchment.shp"
# huc18 = "L:\NHDPlusV2\NHDPlus18\NHDPlusCatchment\Catchment.shp"


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
        if filename_new.endswith('HUC12'):
            filename_new = filename_new
        else:
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
