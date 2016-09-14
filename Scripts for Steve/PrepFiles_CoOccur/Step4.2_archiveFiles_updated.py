import os
import datetime
import arcpy
# Tile: Archived files that were updated- uses output from checkDuplicate, be sure to buffer and merge any point or
# line files before executing.

# User input variable
masterlist = 'J:\Workspace\ESA_Species\ForCoOccur\Documents_FinalBE\MasterListESA_June2016_20160725.csv'

infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'
archivefolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final\Archived'

skipgroup = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Crustaceans',
             'Ferns and Allies', 'Fishes',  'Insects', 'Lichens', 'Mammals', 'Reptiles']
# list of species with updated from from the Check Duplicates script
species_dulplicate_file = ['4090', '74', '81', '143', '1207', '1200', '1202', '1196', '1194', '1205', '1193', '1198',
                           '736', '951', '741', '765', '725', '727', '1187', '745', '565', '1128', '1129', '961',
                           '1132', '767', '769', '770', '771', '774', '10229', '2970', '779', '782', '2683', '795',
                           '2934', '10230', '7229', '1163', '10231', '4740', '800', '983', '806', '815', '601', '1093',
                           '1066', '10232', '10233', '1068', '4030', '605', '993', '999', '829', '830', '832', '1154',
                           '1155', '10234', '846', '848', '849', '850', '862', '4238', '869', '621', '635', '7617',
                           '8277', '8338', '645', '646', '648', '650', '6632', '654', '7805', '659', '671', '1097',
                           '1098', '672', '1188', '674', '7892', '1185', '1101', '10222', '1102', '684', '1186',
                           '10223', '1968', '1051', '10076', '7206', '4420', '619', '10224', '1103', '10225', '685',
                           '10227', '2860', '1104', '6303', '5956', '1131', '1032', '1108', '10228', '2085', '1230',
                           '1349', '1114', '1116', '10235', '1117', '717', '2758', '3653', '939', '719', '721', '731',
                           '732', '947', '3412', '10147', '5210']



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
def archive_dup_files(date_dict, location_dict, archive_folder,archive_list):
    more_than_two_file = []
    same_date = []
    for entid in archive_list:
        try:
            unsorted_date_list = date_dict[entid] # list of dates in order info loaded needed for indexing of files
            sorted_date_list = sorted(date_dict[entid]) # sorted dates to determine which file is older

            # There should only ever be two dates when archive if there is more we need to determine why before archive
            if len(unsorted_date_list) == 2:
                print unsorted_date_list
                # the dates on the files should be different if not we need to determine why before moving forward
                if sorted_date_list[0] != sorted_date_list[1]:
                    oldest_file = sorted_date_list[0] # after sort oldest date will be me in pos 0
                    archived_date = sorted_date_list[1]#after sort date of update  will be me in pos 1

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

dup_date_dict, dup_location_dict = find_dup_files(alpha_group, infolder, skipgroup,species_dulplicate_file)
multiple_archives, same_date = archive_dup_files(dup_date_dict, dup_location_dict, archivefolder,species_dulplicate_file)

print 'Species that have more than one file to archive {0}'.format(multiple_archives)
print 'Species that have multiple files with the same date {0}'.format(same_date)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
