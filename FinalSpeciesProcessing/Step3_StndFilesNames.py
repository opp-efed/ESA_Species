import os
import csv
import datetime

import arcpy


# #Tile: Copy shapefiles to a geodatabase and rename to a standard naming convention.

# NOTE prior to running the NmChange check the the concatenated specode and vipcode on the FWS file name and the
# sci names match the master list.  Files were sometime received with typos

# TODO update so that check against the sci name and Concat codes are scripted
# TODO create the NmChange Dict dynamically from masters using Pandas

# Variables to be set by user
# Input File Locations
# Set the workspace for the ListFeatureClass function
#


InGDB = r"C:\WorkSpace\Processed_20160906\FWS_Ranges_20160906\GDB\FWS_poly_20160906_2016-09-06.gdb"
abb = "FWS"  # FWS or NMFS

# Workspace
ws = "C:\WorkSpace\Processed_20160906"
# Folder in workspace where outputs will be saved
name_dir = "FWS_Ranges_20160906"

# in yyyymmdd received date
receivedDate = '20160906'

# NOTE prior to running the NmChange check the the concatenated specode and vipcode on the FWS file name and the
# sci names match the master list.  Files were sometime received with typos

# #DICT for name change original file to EPA Std
NmChangeDICT = {'usfws_C00S_V12_Chelonia_mydas_area_of_influence': 'R_11192_poly_20160906',
                'usfws_I0T7_I01_Euchloe_ausonides_insulanus_area_of_influence': 'R_5610_poly_20160906',
                'usfws_K03K_I01_Cambarus_callainus_area_of_influence': 'R_5153_poly_20160906',
                'usfws_K08X_I01_Cambarus_veteranus_area_of_influence': 'R_11201_poly_20160906',
                'Q28N_P01_Daphnopsis_hellerana': 'R_921_poly_20160906',
                'Asplenium_diellaciniatum': 'R_10586_poly_20160906',
                'Bulbophyllum_guamense': 'R_10719_poly_20160906',
                'Cyanea_kauaulaensis': 'R_10588_poly_20160906',
                'Cycas_micronesica': 'R_10729_poly_20160906',
                'Dendrobium_guamense': 'R_10720_poly_20160906',
                'Deparia_kaalaana': 'R_10587_poly_20160906',
                'Emballonura_semicaudata_rotensis': 'R_8166_poly_20160906',
                'Emoia_slevini': 'R_10732_poly_20160906',
                'Eugenia_bryanii': 'R_10721_poly_20160906',
                'Exocarpos_menziesii': 'R_10583_poly_20160906',
                'Hedyotis_megalantha': 'R_10722_poly_20160906',
                'Hemignathus_affinis': 'R_11333_poly_20160906',
                'Heritiera_longipetiolata': 'R_3999_poly_20160906',
                'Hypolepis_hawaiiensis_mauiensis': 'R_10594_poly_20160906',
                'Hypolimnas_octocula_marianensis': 'R_4308_poly_20160906',
                'Ischnura_luta': 'R_9282_poly_20160906',
                'Kadua_haupuensis': 'R_10592_poly_20160906',
                'Labordia_lorenciana': 'R_10599_poly_20160906',
                'Lepidium_orbiculare': 'R_10593_poly_20160906',
                'Maesa_walkeri': 'R_10723_poly_20160906',
                'Nervilia_jacksoniae': 'R_10724_poly_20160906',
                'Partula_gibba': 'R_2364_poly_20160906',
                'Partula_langfordi': 'R_7731_poly_20160906',
                'Partula_radiolata': 'R_7907_poly_20160906',
                'Phyllanthus_saffordii': 'R_10725_poly_20160906',
                'Prittchardia_bakeri': 'R_10590_poly_20160906',
                'Pritchardia_lanigera': 'R_3054_poly_20160906',
                'Psychotria_malaspinae': 'R_10726_poly_20160906',
                'Samoana_fragilis': 'R_1862_poly_20160906',
                'Santalum_involutum': 'R_10584_poly_20160906',
                'Schiedea_diffusa_diffusa': 'R_10591_poly_20160906',
                'Sicyos_lanceoloideus': 'R_10585_poly_20160906',
                'Solanum_guamense': 'R_10727_poly_20160906',
                'Tabernaemontana_rotensis': 'R_1266_poly_20160906',
                'Tinospora_homosepala': 'R_11340_poly_20160906',
                'Tuberolabium_guamense': 'R_10728_poly_20160906',
                'Vagrans_egistina': 'R_5168_poly_20160906',
                'Miami_tiger_beetle': 'R_10909_poly_20160906',
                'Suwannee_moccasinshell_': 'R_7372_poly_20160906',

                }


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield (fc)


# creates directories to save files
def create_directory(path_dir, outLocationCSV, OutFolderGDB):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)
    if not os.path.exists(outLocationCSV):
        os.mkdir(outLocationCSV)
        print "created directory {0}".format(outLocationCSV)
    if not os.path.exists(OutFolderGDB):
        os.mkdir(OutFolderGDB)
        print "created directory {0}".format(OutFolderGDB)


# creates date stamped generic file
def create_flnm_timestamp(namefile, outlocation, date_list, file_extension):
    file_extension.replace('.', '')
    filename = str(namefile) + "_" + str(date_list[0]) + '.' + file_extension
    filepath = os.path.join(outlocation, filename)
    return filename, filepath


# outputs table from list generated in create FileList
def create_out_table(list_name, csv_name):
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loops through inGDB and makes a copy of each file applying the std filename from the dict in outGDB
def StandName(InGDB, outFilegdbpath, outFilefailgdbpath):
    for fc in fcs_in_workspace(InGDB):
        try:
            dsc = arcpy.Describe(fc)
            sr = dsc.spatialReference
            prj = sr.name.lower()
            NewName = NmChangeDICT[fc]
            addSRList = str(fc) + "," + "Name: " + sr.name + "," + "Type: " + sr.type + "," + str(NewName)
            print addSRList
            OrgSRList.append(addSRList)
            outFeatureClass = os.path.join(outFilegdbpath, NewName)
            # print outFeatureClass
            if not arcpy.Exists(outFeatureClass):
                # print "FC does not exist"
                arcpy.CopyFeatures_management(fc, outFeatureClass)
            else:
                print" FC already exists"

        except:
            print "Failed  " + str(fc)
            addFailed = str(fc)
            FailedList.append(addFailed)
            outFailedFC = os.path.join(outFilefailgdbpath, addFailed)
            arcpy.CopyFeatures_management(fc, outFailedFC)


# static variable no user input needed unless changing code structure
datelist = []
today = datetime.date.today()
datelist.append(today)

OrgSRList = []
FailedList = []
addSRList = "Filename Original (GDB)" + "," + "Original Projection" + "," + "Original Projection Type" + "," + \
            "Standardize Filename"
OrgSRList.append(addSRList)
addFailed = "Filename-Original (GDB)"
FailedList.append(addFailed)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

# Output File Names
ReNmCSVCopied = 'ReNm' + str(abb) + "_" + str(receivedDate)
out_nameGDB = "ReNm_" + str(abb) + "_" + str(receivedDate)
FailedGDB = "Failed_" + str(out_nameGDB)
ReNmCSVFailed = "Failed_" + str(ReNmCSVCopied)

# CREATES FILE NAMES
# CSV out table succeed and faile
csvfile, csvpath = create_flnm_timestamp(ReNmCSVCopied, outLocationCSV, datelist, 'csv')
failedcsv, failedcsvpath = create_flnm_timestamp(ReNmCSVFailed, outLocationCSV, datelist, 'csv')
# GDB succeed and faile
OutGDB, outFilegdbpath = create_flnm_timestamp(out_nameGDB, OutFolderGDB, datelist, 'gdb')
FailGDB, outFilefailgdbpath = create_flnm_timestamp(FailedGDB, OutFolderGDB, datelist, 'gdb')

arcpy.env.scratchWorkspace = ""
# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True

# Start script

# Copy shapefiles to a file geodatabase and rename

# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(path_dir, outLocationCSV, OutFolderGDB)

create_gdb(OutFolderGDB, OutGDB, outFilegdbpath)
create_gdb(OutFolderGDB, FailGDB, outFilefailgdbpath)
StandName(InGDB, outFilegdbpath, outFilefailgdbpath)

# ##write data store in lists to out tables in csv format
create_out_table(OrgSRList, csvpath)
create_out_table(FailedList, failedcsvpath)
# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
