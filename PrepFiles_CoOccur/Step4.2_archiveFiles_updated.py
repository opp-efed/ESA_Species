import os
import datetime
import arcpy
# Tile: Archived files that were updated- uses output from checkDuplicate, be sure to buffer and merge any point or
# line files before executing.

# User input variable
masterlist = 'J:\Workspace\ESA_Species\ForCoOccur\Documents_FinalBE\MasterListESA_June2016_20160725.csv'

infolder = 'J:\Workspace\ESA_Species\Range\NAD83'
archivefolder = 'J:\Workspace\ESA_Species\Range\NAD83\ArchivedRange'

skipgroup = ['Birds', 'Clams', 'Conifers and Cycads', 'Corals',
             'Lichens', 'Reptiles']

# infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'
# archivefolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final\Archived'

# skipgroup = ['Amphibians', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Crustaceans',
#             'Ferns and Allies', 'Insects', 'Lichens', 'Mammals', 'Reptiles']
# list of species with updated from from the Check Duplicates script
species_dulplicate_file = ['7847', '5065', '468', '7512', '9490', '8356', '320', '6223', '321', '5856', '322', '2316',
                           '8349', '324', '7091', '9487', '5718', '9492', '9488', '333', '2192', '9493', '3226', '9499',
                           '5833', '9500', '9496', '9498', '1897', '5715', '9491', '1905', '9497', '2308', '9489',
                           '362', '9494', '325', '326', '329', '347', '4411', '4210', '369', '318', '327', '330', '351',
                           '360', '7363', '376', '379', '381', '382', '383', '3833', '1680', '336', '349', '350', '354',
                           '7177', '7349', '385', '319', '323', '4042', '9495', '7816', '356', '386', '334', '339',
                           '344', '346', '355', '359', '364', '375', '348', '365', '1369', '342', '337', '370', '1559',
                           '2917', '317', '328', '331', '353', '357', '358', '361', '366', '367', '368', '371', '372',
                           '373', '374', '378', '380', '332', '335', '338', '340', '341', '345', '384', '4086', '352',
                           '4490', '377', '5281', '3645', '6062', '6534', '6841', '7949', '363', '343', '479', '478',
                           '10586', '10594', '10587', '2142', '231', '2599', '1934', '9061', '9505', '245', '9504',
                           '4496', '2956', '9506', '5981', '9503', '8921', '305', '6503', '9502', '236', '242', '244',
                           '253', '254', '264', '269', '274', '276', '277', '280', '293', '297', '300', '307', '7150',
                           '220', '210', '211', '212', '8389', '216', '217', '223', '224', '225', '226', '227', '228',
                           '229', '232', '6557', '241', '243', '235', '238', '239', '6662', '246', '247', '248', '251',
                           '255', '259', '263', '266', '268', '271', '272', '278', '285', '294', '316', '4248', '10060',
                           '284', '282', '281', '308', '312', '313', '315', '3525', '4431', '9220', '219', '279', '214',
                           '250', '256', '262', '292', '10037', '215', '283', '298', '258', '261', '265', '267', '295',
                           '306', '3596', '3879', '314', '8442', '10910', '218', '275', '288', '291', '5719', '7332',
                           '230', '290', '10052', '213', '221', '222', '209', '270', '273', '296', '299', '301', '233',
                           '240', '309', '3280', '249', '7670', '8561', '311', '6297', '287', '234', '237', '257',
                           '260', '252', '3497', '303', '10588', '10583', '10592', '10593', '10590', '10584', '10591',
                           '10585', '3999', '1266', '921', '4308', '5168', '441', '8166', '7731', '1862', '2364',
                           '7907', '3842', '9507', '396', '402', '392', '3364', '414', '416', '2561', '411', '415',
                           '417', '8434', '407']


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
            group = line[7]
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
        print "\nWorking on {0}".format(group)
        group_gdb = in_folder + os.sep + str(group) + '.gdb'

        arcpy.env.workspace = group_gdb
        fclist = arcpy.ListFeatureClasses()

        for fc in fclist:
            ent_id = fc.split('_')
            entid = str(ent_id[1])

            if entid in archive_list:
                print entid
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
            pass
    return more_than_two_file, same_date


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

grouplist = extract_info_master(masterlist)

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)
print alpha_group

dup_date_dict, dup_location_dict = find_dup_files(alpha_group, infolder, skipgroup, species_dulplicate_file)
multiple_archives, same_date = archive_dup_files(dup_date_dict, dup_location_dict, archivefolder,
                                                 species_dulplicate_file)

print 'Species that have more than one file to archive {0}'.format(multiple_archives)
print 'Species that have multiple files with the same date {0}'.format(same_date)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
