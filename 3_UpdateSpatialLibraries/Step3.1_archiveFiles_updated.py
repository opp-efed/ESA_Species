import os
import datetime
import arcpy
import pandas as pd

# Tile: Archived files that were updated- uses output from checkDuplicate, be sure to buffer and merge any point or
# line files before executing.  Generate a look up dictionary of the update date for the file based on the entity ids in
# the species_dulplicate_file list.  After generating the look-up, the older files is archived.  Output will tell you
# if there are multiple files to archive and no archive will occur.  Archive only occurs if there are two files with
# different dates.

# NOTE Files that are being replaced with multiple files must be manually archived before running!!!!!
# TODO find automated way to address issue of files being updated by multiple files; perhaps they should be merged
# before moving them to the spatial libraries

# User input variable
masterlist = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Feb2017_20190130.csv"
group_colindex = 16
infolder = r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\\CriticalHabitat'
archivefolder = r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\\CriticalHabitat\Archived'

skip_group = []

# all groups
# ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Crustaceans','Ferns and Allies',
# 'Flowering Plants', 'Insects', 'Lichens', 'Mammals', 'Reptiles', 'Snails']

# list of species with updated from from the Check Duplicates script Step 4.0; only include species that have a
# replacement file.  If files should be merged together, then it should continue through to step 4.3
# NOTE Files that are being replaced with multiple files must be manually archived before running!!!!!

species_dulplicate_file = ['10517', '1707', '8231', '199', '194', '196', '190', '4773', '9943', '3849', '203', '204',
                           '207', '206', '208', '7610', '9378', '5434', '205', '1740', '6346', '4090', '8395', '142',
                           '132', '131', '130', '137', '1221', '4136', '81', '85', '149', '74', '79', '123', '4237',
                           '129', '66', '67', '6522', '1241', '117', '110', '6901', '118', '4296', '4064', '145', '150',
                           '318', '4086', '7349', '379', '4042', '378', '371', '7363', '4411', '344', '7177', '6534',
                           '366', '367', '365', '4210', '3833', '382', '380', '381', '384', '385', '386', '3645',
                           '7949', '6841', '1559', '357', '355', '383', '353', '370', '373', '1369', '375', '377',
                           '358', '7177', '372', '354', '494', '495', '490', '491', '492', '493', '1261', '6596',
                           '8172', '482', '477', '2268', '1193', '1194', '1197', '1196', '1198', '9962', '9963', '1211',
                           '7529', '1208', '1205', '1207', '1200', '1202', '5658', '4992', '2842', '316', '3654', '272',
                           '273', '4112', '239', '3280', '252', '237', '7834', '7670', '6557', '2528', '7590', '6220',
                           '7150', '287', '2514', '4274', '3596', '301', '2448', '6662', '5265', '247', '246', '7989',
                           '6843', '8241', '9021', '5180', '1509', '9220', '5719', '4799', '258', '296', '5815', '299',
                           '294', '292', '293', '290', '291', '274', '275', '276', '279', '238', '234', '236', '8241',
                           '7590', '243', '242', '249', '248', '3525', '209', '312', '311', '259', '253', '250', '251',
                           '256', '257', '6297', '255', '4799', '215', '288', '281', '280', '283', '282', '285', '284',
                           '286', '263', '262', '266', '264', '309', '301', '305', '306', '228', '7834', '10077',
                           '2118', '10290', '8303', '810', '814', '815', '3990', '719', '717', '712', '710', '7046',
                           '6845', '1069', '1068', '1065', '1067', '1066', '1128', '1086', '594', '1081', '1126',
                           '1088', '3671', '738', '2823', '3387', '3388', '527', '522', '523', '1016', '1400', '1010',
                           '7886', '1230', '1233', '589', '903', '4420', '641', '1108', '648', '3154', '2619', '8347',
                           '745', '850', '851', '858', '859', '2683', '6617', '968', '5956', '4179', '1152', '2860',
                           '741', '743', '559', '558', '746', '747', '554', '7170', '1050', '1051', '1054', '1058',
                           '2458', '1693', '1175', '619', '1171', '610', '611', '614', '1178', '1881', '1283', '947',
                           '941', '688', '685', '684', '687', '686', '623', '1349', '496', '497', '2154', '939', '938',
                           '1378', '930', '3832', '4030', '8336', '8338', '4487', '829', '827', '822', '1177', '709',
                           '1176', '618', '10233', '10232', '10231', '1174', '10235', '10234', '795', '4724', '586',
                           '584', '1138', '582', '583', '580', '581', '1133', '1132', '1131', '1137', '1136', '1135',
                           '1134', '1179', '1636', '2273', '2278', '518', '511', '510', '514', '1001', '516', '621',
                           '1224', '1223', '626', '1129', '646', '6536', '954', '1502', '973', '972', '656', '654',
                           '650', '3084', '659', '3871', '1148', '869', '868', '863', '862', '8181', '864', '867',
                           '1226', '2758', '887', '886', '884', '888', '2085', '756', '1968', '9959', '9958', '9955',
                           '9954', '9957', '9956', '9951', '759', '9953', '9952', '775', '774', '776', '771', '779',
                           '2970', '1049', '8254', '1140', '1141', '1146', '1147', '1263', '1262', '1267', '662', '660',
                           '769', '1278', '3653', '692', '693', '690', '691', '697', '7617', '540', '546', '545', '548',
                           '549', '993', '999', '4238', '7220', '528', '928', '860', '2265', '926', '7229', '832',
                           '833', '830', '3472', '839', '7367', '2934', '1831', '10228', '10229', '10224', '10225',
                           '10227', '6679', '10222', '10223', '784', '786', '787', '780', '781', '782', '788', '9960',
                           '9961', '604', '572', '3753', '6672', '577', '576', '575', '7054', '6490', '602', '2404',
                           '731', '733', '732', '737', '736', '506', '507', '1030', '1139', '500', '630', '865', '3728',
                           '4377', '964', '965', '961', '962', '963', '1106', '1107', '1104', '1105', '1103', '1101',
                           '2517', '1109', '3737', '1159', '1183', '874', '7206', '870', '871', '1155', '645', '1151',
                           '1099', '6632', '1609', '3540', '1607', '9929', '801', '800', '806', '768', '766', '767',
                           '765', '3049', '1074', '1070', '1071', '1094', '1083', '1097', '7805', '1092', '1093', '674',
                           '1154', '1157', '1156', '1098', '671', '672', '673', '4565', '4630', '6019', '535', '534',
                           '533', '531', '530', '7892', '539', '2778', '983', '981', '986', '987', '7979', '4201',
                           '770', '536', '772', '919', '1521', '915', '917', '916', '6303', '665', '3267', '649',
                           '8357', '847', '846', '840', '849', '848', '4740', '1166', '569', '2810', '755', '560',
                           '561', '562', '758', '564', '565', '566', '567', '1032', '1027', '1021', '1020', '1188',
                           '1189', '1186', '1184', '1185', '1182', '7167', '1180', '1181', '726', '727', '725', '720',
                           '721', '728', '605', '601', '603', '1163', '8277', '634', '635', '958', '951', '952', '955',
                           '563', '1111', '1110', '1113', '1112', '1114', '1117', '1116', '1119', '10076', '591',
                           '1710', '622', '590', '4589', '4858', '1257', '1252', '1259', '446', '445', '436', '435',
                           '432', '431', '10147', '4910', '8503', '458', '450', '451', '453', '454', '7495', '4326',
                           '5067', '3412', '9001', '1253', '438', '439', '1249', '1248', '8083', '1258', '1256', '1254',
                           '1255', '1250', '1251', '426', '7261', '1361', '6231', '33', '37', '35', '34', '24', '27',
                           '28', '29', '12', '18', '5210', '63', '8962', '58', '54', '57', '56', '52', '8683', '8685',
                           '8684', '43', '41', '177', '176', '175', '170', '183', '185', '3271', '164', '165', '166',
                           '162', '163', '1783', '2561', '3364', '1245', '1247', '1246', '406', '1380', '4437', '3876',
                           '4766', '4162', '5362', '6138', '4479', '418', '387']


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


# extract the info needed from master
def extract_info_master(master_list):
    group_list = []
    with open(master_list, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group = line[group_colindex]
            group_list.append(group)
    inputFile.close()
    return group_list


# loops through fc in spatial library and extract information for dup files, dates and location of both files
# stores information in dictionary entid as key
def find_dup_files(group_list, in_folder, skip_group, archive_list):
    date_dict = {}
    location_dict = {}
    for group in group_list:
        if group in skip_group:
            continue
        print "\nChecking for duplicate files in: {0}".format(group)
        group_gdb = in_folder + os.sep + str(group) + '.gdb'

        arcpy.env.workspace = group_gdb
        fclist = arcpy.ListFeatureClasses()

        for fc in fclist:
            ent_id = fc.split('_')
            entid = str(ent_id[1])

            if entid in archive_list:

                parse = fc.split("_")
                date = str(parse[3])
                if date_dict.get(entid) is None:
                    date_dict[entid] = [date]
                    location_dict[entid] = [(group_gdb + os.sep + fc)]
                else:
                    datelist = date_dict[entid]
                    datelist.append(date)
                    locationlist = location_dict[entid]
                    locationlist.append((group_gdb + os.sep + fc))
    return date_dict, location_dict


# use dictionaries from functions above function to determine which file is older and then archives is with the current
# file's date at the end of it
def archive_dup_files(date_dict, location_dict, archive_folder, archive_list):
    more_than_two_file = []
    same_date = []
    for entid in archive_list:
        try:
            unsorted_date_list = date_dict[entid]  # list of dates in order info loaded needed for indexing of files
            num_values = len(unsorted_date_list)
            count = 0
            while count < num_values:
                date_val = unsorted_date_list[count]
                if len(date_val) != 8:
                    update_year = '20' + date_val
                    unsorted_date_list.insert((count), update_year)
                    unsorted_date_list.remove(date_val)
                count += 1

            sorted_date_list = sorted(date_dict[entid])  # sorted dates to determine which file is older

            # There should only ever be two dates when archive if there is more we need to determine why before archive
            if len(unsorted_date_list) == 2:
                print unsorted_date_list
                # the dates on the files should be different if not we need to determine why before moving forward
                if sorted_date_list[0] != sorted_date_list[1]:
                    oldest_file = sorted_date_list[0]  # after sort oldest date will be me in pos 0
                    archived_date = sorted_date_list[1]  # after sort date of update  will be me in pos 1

                    # get index position in unsorted list of the oldest date - this will be use to extract the file
                    # that needs to me archived from the location dictionary which will be in the same index postion
                    index_pos_oldest_file = unsorted_date_list.index(oldest_file)
                    if index_pos_oldest_file == 0:
                        index_pos_current = 1
                    else:
                        index_pos_current = 0
                    # get file path to the oldest file; this file will be archived
                    file_location_oldest_file = location_dict[entid][index_pos_oldest_file]
                    # extract path information used to generate path to archive location
                    path, oldest_file = os.path.split(file_location_oldest_file)
                    in_folder, group_gdb = os.path.split(path)
                    archived_gdb = archive_folder + os.sep + group_gdb
                    # creates archive gdb if it does not exist
                    if not arcpy.Exists(archived_gdb):
                        create_gdb(archivefolder, group_gdb, archived_gdb)

                    out_fc_archive = archived_gdb + os.sep + oldest_file + "_" + archived_date
                    if not arcpy.Exists(out_fc_archive):
                        print "Archived: {0} \nCurrent: {1}\n".format(str(out_fc_archive),
                                                                      location_dict[entid][index_pos_current])
                        arcpy.CopyFeatures_management(file_location_oldest_file, out_fc_archive)
                        arcpy.Delete_management(file_location_oldest_file)
                else:
                    # species with two files with the same date, that need to be looked at
                    same_date.append(entid)
            else:
                # species with more than two files and the same date, that need to be looked at
                more_than_two_file.append(entid)
                print 'species {0} has more than 2 files'.format(entid)
        except KeyError:
            print 'Error'
            pass
    return more_than_two_file, same_date


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df.ix[:, group_colindex]
sp_groups_df = sp_groups_df.drop_duplicates()
alpha_group = sorted(sp_groups_df.values.tolist())

dup_date_dict, dup_location_dict = find_dup_files(alpha_group, infolder, skip_group, species_dulplicate_file)
print '\nStarting Archive'
multiple_archives, same_date = archive_dup_files(dup_date_dict, dup_location_dict, archivefolder,
                                                 species_dulplicate_file)

print 'Species that have more than one file to archive {0}'.format(multiple_archives)
print 'Species that have multiple files with the same date {0}'.format(same_date)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
