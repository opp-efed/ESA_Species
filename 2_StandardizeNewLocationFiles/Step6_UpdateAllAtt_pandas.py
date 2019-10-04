import os
import datetime

import arcpy
import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title : Updates all fc to std att needed for composite files using the master list.

# This script is meant for the indv spe file not composite; it will dissolve the files with multiple rows
# this is a different script to update all atts of the composites

# TODO This can be optimize by att loading data into an array rather than dicts
# TODO update files so that character limits in the attribute table doesn't block data update
# TODO set up so that only the incorrect col is deleted and updated


# inputs
# Master species list as .csv
masterlist = r"\MasterListESA_Feb2017_20190130.csv"
# folder or GDB from step 5
infolder = r'path\filename.gdb'


addition_gdb_filename = ''  # if the gdbs are not just the species group
# Species groups that do not need to be run because check was already completed
skiplist = []

# cols from master that should be included use to pop att table
col_included = ['EntityID', 'Common Name', 'Scientific Name', 'spcode', 'vipcode', 'Status', 'pop_abbrev']
final_fields = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status', 'Pop_Abb']  # in order
# order of fields in fc;  used to check if FC has fields in the correct order for comp merge
final_fieldsindex = dict(NAME=2, Name_sci=3, SPCode=4, VIPCode=5, EntityID=1, Status=6,
                         FileName=0, Pop_Abb=7)

# the columns from the final fields list to the corresponding field from the master list
masterlist_fc_dict = {'NAME': 'Common Name', 'Name_sci': 'Scientific Name', 'SPCode': 'spcode', 'VIPCode': 'vipcode',
                      'Status': 'Status', 'Pop_Abb': 'pop_abbrev'}


# Static variables

entid_indexfilenm = 1  # NOTE NOTE HARD CODE TO THE LOCATION OF THE ENTITYID IN THE FILENAME
updatefiles = False
singleGDB = False
DissolveFiles = False
extention = infolder.split(".")
cnt = len(extention)
if cnt > 1:
    if extention[1] == 'gdb':
        singleGDB = True
    else:
        singleGDB = False
else:
    singleGDB = False

start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


# Functions
# pulls sp info from masterlist
def extract_species_info(master_in_table, col_from_master):
    if master_in_table.split('.')[1] == 'csv':
        master_list_df = pd.read_csv(master_in_table)
    else:
        master_list_df = pd.read_excel(master_in_table)

    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    # Extracts on the columns set by user into a df to be used in the script
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    # Extracts a list of species groups for looking across gdbs
    group_df = master_list_df['Group']
    unq_groups = group_df.drop_duplicates()
    del master_list_df
    return sp_info_df, unq_groups


# print update output
def output_update(fc, value):
    print "     Updated {0} for files {1}".format(value, fc)


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# check all att to see if there is an updated value on the current master
def check_att_values_update_changes(fc, entid_filename, spe_info_df, masterlist_fc_dict, ent_filename_fail_list,
                                    filename_updated, final_fields):
    completed_fc = True
    do_not_update = False
    # loop through all the final fields compares value back to the master and if it is different then updates it

    total_fields = (len(final_fields))
    field_order_count = 0
    single_sp = spe_info_df[spe_info_df['EntityID'].isin([entid_filename]) == True]
    # Checks values for each col in att, and compares back to master list if they are not the same the att  is updated
    while total_fields > field_order_count:
        field = final_fields[field_order_count]
        with arcpy.da.UpdateCursor(fc, field) as cursor:
            if field == 'FileName':
                for row in cursor:
                    current = str(row[0])
                    if current is None:
                        value = str(fc)
                        row[0] = value
                        cursor.updateRow(row)
                        output_update(fc, field)
                        field_order_count += 1
                    elif current != str(fc):
                        value = str(fc)
                        row[0] = value
                        try:
                            cursor.updateRow(row)
                            output_update(fc, field)
                        except:
                            arcpy.DeleteField_management(fc, field)
                            print 'Deleted Filname re-run  update'
                            completed_fc = False
                            # could not update due to field length, so deleted field then re added and populated
                            # Bool to re-add field and pop
                            filename_updated.append(entid_filename)
                        field_order_count += 1

                    else:
                        field_order_count += 1
                        continue
                continue
            if field == 'EntityID':
                col_index = col_included.index('EntityID')
                value = single_sp.iloc[0, col_index]

                for row in cursor:
                    current = row[0]

                    if current is None:
                        row[0] = value
                        cursor.updateRow(row)
                        output_update(fc, field)
                        field_order_count += 1
                    elif current != entid_filename:
                        print 'EntityID does not match filename in file {0}'.format(fc)
                        do_not_update = True  # This will break the while loop and stop the script to figure out why
                        # This is done to make sure the files and the atts are for the same and the correct species
                        ent_filename_fail_list.append(entid_filename)
                        field_order_count += 1

                    else:
                        field_order_count += 1
                        continue
            else:
                if not do_not_update:
                    field_col_master = masterlist_fc_dict[field]
                    col_index = col_included.index(field_col_master)

                    value = single_sp.iloc[0, col_index]
                    for row in cursor:
                        current = row[0]
                        if current != value:
                            row[0] = value
                            try:
                                cursor.updateRow(row)
                                output_update(fc, field)
                                field_order_count += 1
                            except:
                                arcpy.DeleteField_management(fc, field)
                                # could not update due to field length, so deleted field then re added and populated
                                # Bool to re-add field and pop
                                completed_fc = False
                                print 'Deleted col re-run update'
                                filename_updated.append(entid_filename)
                                field_order_count += 1
                        else:
                            field_order_count += 1

                else:
                    break

    return filename_updated, ent_filename_fail_list, do_not_update, completed_fc


# verifies field order for att; if in correct deletes fields and repopulates
def CheckFieldOrder(inGDB, final_fieldsindex, DissolveFiles, ent_filename_fail_list, filename_updated, spe_info_df,
                    masterlist_fc_dict):
    group_gdb = inGDB
    arcpy.env.workspace = group_gdb
    print group_gdb
    fclist = arcpy.ListFeatureClasses()
    checkorder = {}
    counter = len(fclist)
    do_not_update = False

    for fc in fclist:
        print '{0} remaining... {1}'.format(fc, counter - 1)
        result = arcpy.GetCount_management(fc)
        count = int(result.getOutput(0))


        if count > 1:
            DissolveFiles = True

        entid = fc.split('_')
        entid = str(entid[entid_indexfilenm])  # based on static variable

        # Boolean for while loop - breaks
        completed_fc = False
        do_not_update = False
        order_correct = True

        while not completed_fc:
            fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
            # Check to see if any final field is missing based on length user inputted list lengths must match
            if len(fclist_field) != (len(final_fields)):
                order_correct = False
            else:
                # Check to see if the fields in the att table are in the correct order
                for field in fclist_field:
                    i = fclist_field.index(field)
                    checkorder[field] = i
                    try:
                        j = final_fieldsindex[field]
                        if i != j:
                            order_correct = False
                        else:
                            continue
                    except KeyError:  # deletes extraneous fields in fc that would return Key Error b/c they are not in
                        # the dictionary
                        arcpy.DeleteField_management(fc, field)
            # if order is not correct or field to be add, all fields are deleted then updated base don current master
            if not order_correct:
                for field in fclist_field:
                    arcpy.DeleteField_management(fc, field)
                for field in final_fields:
                    arcpy.AddField_management(fc, field, "TEXT")
                    print "added field {1} for {0}".format(fc, field)
            # Check values in att table against master and update as need
            filename_updated, ent_filename_fail_list, do_not_update, completed_fc = check_att_values_update_changes \
                (fc, entid, spe_info_df, masterlist_fc_dict, ent_filename_fail_list, filename_updated, final_fields)
            if do_not_update:
                # safety check entid value in att table should never be different that the filename
                'Species entid in att table and filename do not match for'.format(ent_filename_fail_list)
                break
        # deletes any extra columns
        fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
        if not do_not_update:
            for field in fclist_field:
                if field not in final_fields:
                    arcpy.DeleteField_management(fc, field)
                else:
                    pass
        counter -= 1
    return DissolveFiles, filename_updated, ent_filename_fail_list, do_not_update


# dissolves files as needed
def dissolveloop(inGDB, final_fields):
    # Dissolve indv species files so that it is a single row that can be loaded into a comp file
    group_gdb = inGDB
    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        result = arcpy.GetCount_management(fc)
        count = int(result.getOutput(0))
        if count > 1:
            infc = inGDB + os.sep + fc
            path, tail = os.path.split(inGDB)
            outname = "Dissolve" + tail
            outGDB = path + os.sep + outname
            if not os.path.exists(outGDB):
                create_gdb(path, outname, outGDB)
            outFC = outGDB + os.sep + str(fc)
            if not arcpy.Exists(outFC):
                arcpy.Dissolve_management(infc, outFC, final_fields)
                print "Dissolved {0}".format(fc)
            else:
                continue


sp_df, group_df = extract_species_info(masterlist, col_included)

filename_needed_updated = []
ent_does_not_match_filename = []
if singleGDB:
    inGDB = infolder
    DissolveFiles, filename_needed_updated, ent_does_not_match_filename, do_not_update = CheckFieldOrder \
        (inGDB, final_fieldsindex, DissolveFiles, ent_does_not_match_filename, filename_needed_updated, sp_df,
         masterlist_fc_dict)

    if not do_not_update:
        if DissolveFiles:
            dissolveloop(inGDB, final_fields)
        DissolveFiles = False
else:
    alpha_group = sorted(group_df.values.tolist())
    for group in alpha_group:
        start_loop = datetime.datetime.now()
        if group in skiplist:
            continue
        print "\nWorking on {0}".format(group)
        inGDB = infolder + os.sep + str(group) + str(addition_gdb_filename) + '.gdb'
        if not arcpy.Exists(inGDB):
            continue
        DissolveFiles, filename_needed_updated, ent_does_not_match_filename, do_not_update = CheckFieldOrder \
            (inGDB, final_fieldsindex, DissolveFiles, ent_does_not_match_filename, filename_needed_updated, sp_df,
             masterlist_fc_dict)
        if not do_not_update:
            if DissolveFiles:
                dissolveloop(inGDB, final_fields)
            DissolveFiles = False
        else:
            break
        endloop = datetime.datetime.now()
        print "Elapse time {0}".format(endloop - start_script)
print 'Species with a filename that needs update {0}'.format(filename_needed_updated)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
