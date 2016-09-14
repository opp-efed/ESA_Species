import os

import time
import datetime
import csv
import functions
import arcpy

# TODO
# possibly speed by loading data into arrays?
# write to a CSV at end for each rather than using table to table conversion
# add code to export what is "print" to a txt so that the selection numbers can be Qaed

# User variable
# Workspace

# NOTE NOTE if the script is stop by user a table can be partial completed- this needs to be deleted before restarting
# if script stops due to error partial table will be deleted

ws = "C:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\SteamCrosswalk"
name_dir = "All_species_inaBin_Master_20160819"

# GDB with all Composites to Run
MasterSpeFC = r"C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_SpGroupComposite.gdb"
#MasterSpeFC = r"C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite.gdb"
# File type to flag the species with
path, gdb = os.path.split(MasterSpeFC)
file_type = gdb.split('_')[0]
# out csv for species without any streams
noNHDCSV = "FWS_" + str(file_type) + "_l48_noNHD2_b"

# name of field that will ID the HUC 2 and location of that file, first all of the HUC2 are identifies so that the
# correct folder from the NHD can be checked
HUC2Field = "VPU_ID"
HUC2_lwr48 = "C:\Workspace\ESA_Species\StreamCrosswalk\VPU.gdb\VPU_Final"

# Stream files, the HUC 2 number needs to match the ID field above
huc1 = "J:\NHDPlusV2\NHDPlus01\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc2 = "J:\NHDPlusV2\NHDPlus02\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3N = "J:\NHDPlusV2\NHDPlus03N\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3S = "J:\NHDPlusV2\NHDPlus03S\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3W = "J:\NHDPlusV2\NHDPlus03W\NHDSnapshot\Hydrography\NHDFlowline.shp"
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
# List of species to consider
species_to_run = ['209', '210', '211', '212', '213', '214', '215', '216', '218', '219', '220', '221', '222', '223',
                  '224',
                  '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '237', '238', '239', '240',
                  '241',
                  '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '254', '255', '256',
                  '257',
                  '258', '259', '260', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272',
                  '273',
                  '274', '275', '276', '277', '278', '279', '280', '281', '282', '283', '284', '285', '286', '287',
                  '288',
                  '290', '291', '292', '293', '294', '295', '296', '297', '298', '299', '300', '301', '303', '305',
                  '306',
                  '307', '308', '309', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321',
                  '322',
                  '323', '324', '325', '326', '327', '328', '329', '330', '331', '332', '333', '334', '335', '336',
                  '337',
                  '338', '339', '340', '341', '342', '343', '344', '345', '346', '347', '348', '349', '350', '351',
                  '352',
                  '353', '354', '355', '356', '357', '358', '359', '360', '361', '362', '363', '364', '365', '366',
                  '367',
                  '368', '369', '370', '371', '372', '373', '374', '375', '376', '377', '378', '379', '380', '381',
                  '382',
                  '383', '384', '385', '386', '402', '407', '411', '414', '415', '416', '417', '441', '478', '479',
                  '1369',
                  '1509', '1559', '1680', '1897', '1905', '1934', '2142', '2192', '2308', '2316', '2448', '2514',
                  '2528',
                  '2561', '2599', '2842', '2917', '2956', '3226', '3280', '3364', '3398', '3497', '3525', '3596',
                  '3645',
                  '3654', '3833', '3842', '3879', '4042', '4086', '4093', '4112', '4210', '4248', '4274', '4300',
                  '4330',
                  '4411', '4431', '4490', '4496', '4799', '4881', '4992', '5065', '5180', '5265', '5281', '5658',
                  '5715',
                  '5718', '5719', '5815', '5833', '5856', '5981', '6062', '6220', '6223', '6297', '6503', '6534',
                  '6557',
                  '6578', '6662', '6841', '6843', '6966', '7091', '7150', '7177', '7332', '7349', '7363', '7512',
                  '7590',
                  '7670', '7816', '7834', '7847', '7855', '7949', '7989', '8241', '8278', '8349', '8356', '8389',
                  '8434',
                  '8442', '8561', '8921', '9021', '9061', '9220', '9487', '9488', '9489', '9490', '9491', '9492',
                  '9493',
                  '9494', '9495', '9496', '9497', '9498', '9499', '9500', '9502', '9503', '9504', '9505', '9506',
                  '9507',
                  '10037', '10052', '10060', '10077', '10150', '10297', '10298', '10299', '10300', '10301', '10910',
                  '2',
                  '7', '19', '26', '58', '67', '69', '70', '76', '84', '88', '91', '103', '104', '108', '124', '125',
                  '130',
                  '131', '132', '134', '135', '136', '139', '147', '152', '167', '168', '169', '171', '172', '173',
                  '176',
                  '180', '182', '187', '189', '191', '194', '196', '197', '202', '204', '205', '206', '207', '396',
                  '398',
                  '399', '401', '403', '404', '406', '408', '409', '412', '413', '418', '435', '439', '445', '453',
                  '454',
                  '475', '477', '480', '481', '482', '484', '486', '487', '489', '517', '580', '677', '807', '870',
                  '1064',
                  '1199', '1247', '1261', '1302', '1358', '1361', '1380', '1707', '1740', '1783', '1849', '1953',
                  '2144',
                  '2767', '3271', '3628', '4162', '4326', '4437', '4479', '4679', '4766', '4910', '5232', '5362',
                  '5434',
                  '5714', '6138', '6231', '6346', '6596', '6620', '6654', '6739', '7342', '7610', '7800', '8172',
                  '8231',
                  '8621', '8765', '8861', '8962', '9501', '9694', '9967', '9968', '9969', '10038', '10039', '10124',
                  '10130',
                  '10517', '153', '154', '155', '160', '217', '225', '236', '253', '261', '1769', '2510', '2891',
                  '3096',
                  '3133', '3199', '3318', '4719', '5623', '5989', '7115', '8181', '8462', '9126', '9382', '9384',
                  '9707',
                  '9941', '10013', '10142', '10144', '10145', '10151', '10153', '10310', '10311', '10312', '10314',
                  '10319',
                  '10323', '10326', '10332', '10340', '10341', '10370', '10381', '10700', '10733', '10734', '10736',
                  '10903',
                  '10908', 'NMFS125', 'NMFS134', 'NMFS137', 'NMFS138', 'NMFS139', 'NMFS159', 'NMFS166', '11', '12',
                  '17',
                  '18', '23', '24', '28', '29', '45', '46', '52', '60', '61', '65', '71', '72', '73', '82', '85', '93',
                  '96',
                  '102', '110', '114', '115', '117', '119', '120', '123', '137', '143', '145', '146', '149', '163',
                  '170',
                  '188', '190', '195', '199', '201', '203', '208', '400', '442', '443', '447', '448', '449', '457',
                  '460',
                  '463', '464', '465', '466', '467', '476', '483', '485', '488', '490', '491', '492', '493', '494',
                  '495',
                  '498', '521', '522', '527', '530', '531', '534', '554', '558', '566', '568', '570', '582', '583',
                  '585',
                  '592', '593', '594', '595', '596', '611', '612', '625', '647', '653', '655', '656', '660', '667',
                  '668',
                  '678', '679', '711', '723', '743', '748', '753', '764', '785', '786', '787', '790', '813', '818',
                  '819',
                  '823', '828', '837', '854', '858', '859', '875', '881', '891', '896', '906', '912', '914', '927',
                  '931',
                  '941', '946', '960', '967', '976', '977', '982', '984', '991', '994', '995', '997', '1000', '1003',
                  '1005',
                  '1008', '1017', '1028', '1039', '1047', '1057', '1073', '1080', '1081', '1090', '1145', '1164',
                  '1172',
                  '1189', '1203', '1204', '1221', '1225', '1228', '1236', '1237', '1238', '1239', '1245', '1246',
                  '1263',
                  '2782', '2859', '2929', '4064', '4090', '4296', '4773', '5210', '5449', '6097', '6522', '6617',
                  '6618',
                  '6867', '6901', '7482', '8395', '9378', '9709', '9943', '10010', '10290', '10587', '9432', '10485',
                  '11175', '11176', '11191', '11192', '11193', '392', '468', '7372', '11262', 'NMFS173', 'NMFS174',
                  'NMFS175', 'NMFS176', 'NMFS177', 'NMFS178', 'NMFS179', 'NMFS180', 'NMFS181', 'NMFS182 '
                  ]


# timers


def start_times(start_clock):
    start_time = datetime.datetime.fromtimestamp(start_clock)
    print "Start Time: " + str(start_time)
    print start_time.ctime()


def end_times(end_clock, start_clock):
    start_time = datetime.datetime.fromtimestamp(start_clock)
    end = datetime.datetime.fromtimestamp(end_clock)
    print "End Time: " + str(end)
    print end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)


# creates directories
def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        print "created directory {0}".format(dbf_dir)


def stream_crosswalk(in_fc, HUC2_Lowr48, LYR_dir, DBF_dir, HUCdict, NoNHD, csvpath):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    # each row is a species
    # for each species first the HUC 2s are identified then the stream in each HUC are select and export to one master
    # table for the species
    # output is all streams for a species across all HUC2
    for row in arcpy.SearchCursor(in_fc):
        lyr = file_type + "_{0}_lyr".format(row.EntityID)  # layer name for selected features
        out_layer = LYR_dir + os.sep + lyr + ".lyr"
        table = "{1}_Streams_{0}.dbf".format(row.EntityID, file_type)
        temptable = os.path.join(DBF_dir, table)
        if arcpy.Exists(temptable):
            continue

        # NOTE NOTE HARD CODE TO ENTITYID
        entid = "{0}".format(row.EntityID)  # Extract entity ID number
        if entid not in species_to_run:  # skips species not in list
            continue
        if not arcpy.Exists(out_layer):
            whereclause = "EntityID = '%s'" % entid
            print whereclause
            # Makes a feature layer that will only include current entid using whereclause
            arcpy.MakeFeatureLayer_management(in_fc, lyr, whereclause)
            print "Creating layer {0}".format(lyr)
            arcpy.SaveToLayerFile_management(lyr, out_layer, "ABSOLUTE")
            print "Saved layer file"
        if not arcpy.Exists(temptable):
            spec_location = str(out_layer)
            # check for HUC a species occurs in
            arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
            arcpy.Delete_management("slt_lyr")
            arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
            # saves all HUC2 to a list this will have duplicates they are removed using the set below
            with arcpy.da.SearchCursor("slt_lyr", HUC2Field) as cursor:
                HUC2list = sorted({row[0] for row in cursor})

            print HUC2list

            # for each value in the HUC2 set will select all stream that are with species file, and save to a master
            # species table, one table per species will include all values
            counter = 0
            try:
                for z in HUC2list:
                    print z
                    huc_fc = HUCdict[z]
                    print huc_fc

                    arcpy.Delete_management("huc_lyr")
                    arcpy.MakeFeatureLayer_management(huc_fc, "huc_lyr")

                    # NOTE NOTE would a different selection type be better or should multople be used?
                    arcpy.SelectLayerByLocation_management("huc_lyr", "INTERSECT", out_layer, "", "NEW_SELECTION")
                    count = arcpy.GetCount_management("huc_lyr")
                    print str(count) + " selected features"
                    tableview = "tbl_view_" + str(counter)
                    arcpy.Delete_management(tableview)
                    arcpy.MakeTableView_management("huc_lyr", tableview)
                    count = int(arcpy.GetCount_management(tableview).getOutput(0))
                    print str(count) + " Tableview"
                    if count < 1:
                        print 'Zero'
                        filename = str(lyr)
                        if filename not in NoNHD:
                            NoNHD.append(filename)
                        continue

                        # NOTE NOTE if the script is stop and a table was start but not completed for a species it
                        # must be deleted before starting the script again.If a table has been created the script will
                        # move to the next species
                    if counter == 0:  # This will be for first HUC (counter =0) for the species the table is  created
                        if count != 0:
                            filename = str(lyr)
                            arcpy.TableToTable_conversion(tableview, DBF_dir, table)
                            print "created table: " + str(temptable)
                            counter += 1
                            if filename in NoNHD:
                                NoNHD.remove(filename)
                    else:  # remaining results for additional HUC selection values will be appened to table
                        arcpy.Append_management(tableview, temptable, "NO_TEST", "", "")
                        counter += 1
                print "table {0} completed. Located at {1}".format(temptable, DBF_dir)
            except Exception as err:
                print(err.args[0])
                arcpy.Delete_management(temptable)
                print 'Deleted partial table {0}'.format(temptable)

        else:
            print "Stream Crosswalk for {0}".format(lyr) + " previously populated"

    arcpy.Delete_management("HUC48_lyr")

    create_outtable(NoNHD, csvpath)
    return NoNHD


# exports list to csv
def create_outtable(list_name, csv_location):
    with open(csv_location, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


# Creates CSV names with timestamp
def create_csvflnm_timestamp(name_csv_file, out_csv_location):
    filename = str(name_csv_file) + "_" + str(date_list[0]) + '.csv'
    filepath = os.path.join(out_csv_location, filename)
    return filename, filepath


date_list = []
today = datetime.date.today()
date_list.append(today)

# workspace and output paths
File_dir = ws + os.sep + str(name_dir)
LYR_dir = File_dir + os.sep + 'LYR'
DBF_dir = File_dir + os.sep + 'DBF'
csvfile, csvpath = create_csvflnm_timestamp(noNHDCSV, DBF_dir)
noNHD = []  # empty list to hold species without any streams; this is exported to a csv at end

# Set up HUC DICT
HUCdict = {}
for k, v in globals().items():
    if k.startswith('huc'):
        num = k.replace("huc", "")
        HUCdict[num] = v

start = time.time()
start_times(start)

# Create Workspaces
create_directory(File_dir)
create_directory(LYR_dir)
create_directory(DBF_dir)

# Execute loop that will check each input comp fc to see if a species in the species list is present, and if so it will
# extract all streams with in the range and save to a dbf
for fc in functions.fcs_in_workspace(MasterSpeFC):
    check_prj = fc.split('_')
    try:
        if 'AlbersUSA' == check_prj[4]:
            continue
    except:
        pass

    print fc
    input_fc = MasterSpeFC + os.sep + fc
    stream_crosswalk(input_fc, HUC2_lwr48, LYR_dir, DBF_dir, HUCdict, noNHD, csvpath)

print noNHD
done = time.time()
end_times(done, start)
