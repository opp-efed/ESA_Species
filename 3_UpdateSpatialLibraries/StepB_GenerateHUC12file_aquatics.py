import os
import time
import datetime

import arcpy

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# TODO Update to seamless NHDPlus
# NOTE NOTE if the script is stopped and an species that was started but not completed for a species it
# must be deleted before starting the script again.If a table has been created the script will
# move to the next species; could add a try/ except that will delete partial file if script bombs

arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
extractfiles = 'HUC12'

name_dir = "GDB"

###folder used as temp work space

noNHDCSV = extractfiles + "_noNHD_20190204"

# GDB with all files to run
MasterSpeFC = r"path\Generalized files\CriticalHabitat"
# path to temp gdb for huc12 files
out_location = r'path\HUC12'
# temp gdb  for huc12
gdb_name = 'file name.gdb'

# Field id for HUC12 number
HUC2Field = "HUC_12"
# path to final location
HUC2_lwr48 = "path\FilesAppended_ESA.gdb\HUC12_Merge"

FWSaqu_species = ['209', '215', '228', '234', '235', '236', '237', '238', '239', '242', '243', '246', '247', '248',
                  '249', '250', '251', '252', '253', '255', '256', '257', '258', '259', '262', '263', '264', '266',
                  '272', '273', '274', '275', '276', '279', '280', '281', '282', '283', '284', '285', '286', '287',
                  '288', '290', '291', '292', '293', '294', '296', '299', '301', '305', '306', '309', '311', '312',
                  '316', '318', '344', '353', '354', '355', '357', '358', '365', '366', '367', '370', '371', '372',
                  '373', '375', '377', '378', '379', '380', '381', '382', '383', '384', '385', '386', '1369', '1509',
                  '1559', '1934', '2448', '2514', '2528', '2561', '2842', '3280', '3364', '3525', '3596', '3645',
                  '3654', '3833', '4042', '4086', '4093', '4112', '4210', '4274', '4300', '4411', '4799', '4992',
                  '5065', '5180', '5265', '5658', '5719', '5815', '5981', '6220', '6297', '6534', '6557', '6662',
                  '6841', '6843', '7150', '7177', '7349', '7363', '7590', '7670', '7834', '7855', '7949', '7989',
                  '8181', '8241', '9021', '9061', '9220', '9432', '9504', '10060', '10077',

                  ]


def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


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


def streamcrosswalk(fc, HUC2_Lowr48, LYR_dir, outlocation):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    filename = str(fc)

    entid = fc.split("_")[1]
    filename_new = filename + "_" + extractfiles
    outfc = outlocation + os.sep + filename_new
    out_layer = LYR_dir + os.sep + entid + ".lyr"

    if not arcpy.Exists(outfc):
        arcpy.MakeFeatureLayer_management(fc, "lyr")
        print "Creating layer {0}".format(entid)
        arcpy.SaveToLayerFile_management("lyr", out_layer, "ABSOLUTE")
        print "Saved layer file"
        spec_location = str(out_layer)
        arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
        arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
        arcpy.CopyFeatures_management("slt_lyr", outfc)
        arcpy.Delete_management("slt_lyr")


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


def create_gdbflnm_timestamp(namegdbfile, outgdblocation):
    filename = str(namegdbfile) + "_" + str(datelist[0])
    filepath = os.path.join(outgdblocation, (filename + '.gdb'))
    return filename, filepath


def create_csvflnm_timestamp(NameCSVFile, outCSVLocation):
    filename = str(NameCSVFile) + "_" + str(datelist[0]) + '.csv'
    filepath = os.path.join(outCSVLocation, filename)
    return (filename, filepath)


datelist = []
today = datetime.date.today()
datelist.append(today)
File_dir = out_location + os.sep + str(name_dir)
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
createdirectory(GDB_dir)
createdirectory(LYR_dir)
createdirectory(DBF_dir)

out_location_gdb = out_location + os.sep + gdb_name
create_gdb(out_location, gdb_name, out_location_gdb)

if MasterSpeFC.endswith('gdb'):
    path, tail = os.path.split(MasterSpeFC)
    list_gdb = [tail]
else:
    list_gdb = os.listdir(MasterSpeFC)
    list_gdb = [gdb for gdb in list_gdb if gdb.endswith('gdb')]
    path = MasterSpeFC
print list_gdb
for gdb in list_gdb:
    arcpy.env.workspace = path + os.sep + gdb
    fclist = arcpy.ListFeatureClasses()

    create_gdb(out_location, gdb, out_location_gdb)
    for fc in fclist:
        print fc
        entid = fc.split('_')[1]
        if len(FWSaqu_species) > 0:
            if entid in FWSaqu_species:
                streamcrosswalk(fc, HUC2_lwr48, LYR_dir, out_location_gdb)
            else:
                pass

print noNHD
done = time.time()
end_times(done, start)
