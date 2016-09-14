import os
import gc
import time
import datetime
import StreamCrosswalk.functions
import pandas as pd
import arcpy

gc.enable()


# ## to do create streams layers by huc (huc_fc) onces then open it rather than making it for each species
# ##possibly speed up by only extracting information need from table ie reach code, save to a list, append list then
### write to a CSV at end for each rather than using table to table conversion
###add code to export what is "print" to a txt


arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
# removed 286 due to memory issue manuall check
DDspecies = ['2', '7', '19', '26', '58', '67', '69', '70', '76', '84', '88', '91', '103', '104', '108', '124', '125',
             '130', '131', '132', '134', '135', '136', '139', '147', '152', '157', '158', '167', '168', '169', '171',
             '172', '173', '176', '180', '182', '187', '189', '191', '194', '196', '197', '202', '204', '205', '206',
             '207', '209', '210', '211', '212', '213', '214', '215', '216', '218', '219', '220', '221', '222', '223',
             '224', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '237', '238', '239', '240',
             '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '254', '255', '256',
             '257', '258', '259', '260', '262', '263', '264', '265', '266', '267', '268', '269', '270', '271', '272',
             '273', '274', '275', '276', '277', '278', '279', '280', '281', '282', '283', '284', '285', '287',
             '288', '290', '291', '292', '293', '294', '295', '296', '297', '298', '299', '300', '301', '303', '305',
             '306', '307', '308', '309', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320', '321',
             '322', '323', '324', '325', '326', '327', '328', '329', '330', '331', '332', '333', '334', '335', '336',
             '337', '338', '339', '340', '341', '342', '343', '344', '345', '346', '347', '348', '349', '350', '351',
             '352', '353', '354', '355', '356', '357', '358', '359', '360', '361', '362', '363', '364', '365', '366',
             '367', '368', '369', '370', '371', '372', '373', '374', '375', '376', '377', '378', '379', '380', '381',
             '382', '383', '384', '385', '386', '396', '398', '399', '401', '402', '403', '404', '406', '407', '408',
             '409', '411', '412', '413', '414', '415', '416', '417', '418', '435', '439', '441', '445', '453', '454',
             '475', '477', '478', '479', '480', '481', '482', '484', '486', '487', '489', '517', '580', '677', '807',
             '870', '1064', '1199', '1247', '1261', '1302', '1358', '1361', '1369', '1380', '1509', '1559', '1680',
             '1707', '1740', '1783', '1849', '1897', '1905', '1934', '1953', '2142', '2144', '2192', '2308', '2316',
             '2448', '2514', '2528', '2561', '2599', '2767', '2842', '2917', '2956', '3226', '3271', '3280', '3364',
             '3398', '3497', '3525', '3596', '3628', '3645', '3654', '3833', '3842', '3879', '4042', '4086', '4093',
             '4110', '4112', '4162', '4210', '4248', '4274', '4300', '4326', '4330', '4411', '4431', '4437', '4479',
             '4490', '4496', '4679', '4766', '4799', '4881', '4910', '4992', '5065', '5180', '5232', '5265', '5281',
             '5362', '5434', '5658', '5714', '5715', '5718', '5719', '5815', '5833', '5856', '5981', '6062', '6138',
             '6220', '6223', '6231', '6297', '6346', '6416', '6503', '6534', '6557', '6578', '6596', '6620', '6654',
             '6662', '6739', '6841', '6843', '6966', '7091', '7150', '7177', '7332', '7342', '7349', '7363', '7512',
             '7590', '7610', '7670', '7753', '7800', '7816', '7834', '7847', '7855', '7949', '7989', '8172', '8231',
             '8241', '8278', '8349', '8356', '8389', '8434', '8442', '8561', '8621', '8765', '8861', '8921', '8962',
             '9021', '9061', '9220', '9487', '9488', '9489', '9490', '9491', '9492', '9493', '9494', '9495', '9496',
             '9497', '9498', '9499', '9500', '9501', '9502', '9503', '9504', '9505', '9506', '9507', '9694', '9967',
             '9968', '9969', '10037', '10038', '10039', '10052', '10060', '10077', '10124', '10130', '10150', '10297',
             '10298', '10299', '10300', '10301', '10517', '10910', 'NMFS166', 'NMFS88','286']

outfile = 'CheckHUCRangePartial' + '.csv'
fileType = "Range"
outlocation ='C:\Workspace\ESA_Species\StreamCrosswalk'
MasterSpeFC = r"J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\NL48_ProjectedSpGroupComposites.gdb"

HUC2Field = "VPU_ID"
HUC2_lwr48 = "C:\Workspace\ESA_Species\StreamCrosswalk\VPU.gdb\VPU_Final"


# Stream files
huc1 = "E:\NHDPlusV2\NHDPlus01\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc2 = "E:\NHDPlusV2\NHDPlus02\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3N = "E:\NHDPlusV2\NHDPlus03N\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3S = "E:\NHDPlusV2\NHDPlus03S\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc3W = "E:\NHDPlusV2\NHDPlus3W\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc4 = "E:\NHDPlusV2\NHDPlus04\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc5 = "E:\NHDPlusV2\NHDPlus05\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc6 = "E:\NHDPlusV2\NHDPlus06\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc7 = "E:\NHDPlusV2\NHDPlus07\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc8 = "E:\NHDPlusV2\NHDPlus08\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc9 = "E:\NHDPlusV2\NHDPlus09\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc10L = "E:\NHDPlusV2\NHDPlus10L\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc10U = "E:\NHDPlusV2\NHDPlus10U\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc11 = "E:\NHDPlusV2\NHDPlus11\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc12 = "E:\NHDPlusV2\NHDPlus12\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc13 = "E:\NHDPlusV2\NHDPlus13\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc14 = "E:\NHDPlusV2\NHDPlus14\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc15 = "E:\NHDPlusV2\NHDPlus15\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc16 = "E:\NHDPlusV2\NHDPlus16\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc17 = "E:\NHDPlusV2\NHDPlus17\NHDSnapshot\Hydrography\NHDFlowline.shp"
huc18 = "E:\NHDPlusV2\NHDPlus18\NHDSnapshot\Hydrography\NHDFlowline.shp"


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


def streamcrosswalk(fc, HUC2_Lowr48, LYR_dir, specieHUCdict,DDspecies):
    arcpy.MakeFeatureLayer_management(HUC2_Lowr48, "HUC48_lyr")
    for row in arcpy.SearchCursor(fc):
        lyr = fileType + "_{0}_lyr".format(row.EntityID)
        out_layer = LYR_dir + os.sep + lyr + ".lyr"
        id = fileType + "_{0}_lyr".format(row.EntityID)
        id = id[:-4]
        id = id.replace((fileType + "_"), "")
        if id not in DDspecies:
                continue
        print id
        if not os.path.exists(out_layer):
            whereclause = "EntityID = '%s'" % (id)
            print whereclause
            arcpy.MakeFeatureLayer_management(fc, lyr, whereclause)
            print "Creating layer {0}".format(lyr)
            arcpy.SaveToLayerFile_management(lyr, out_layer, "ABSOLUTE")
            print "Saved layer file"
            del lyr

        spec_location = str(out_layer)
        arcpy.SelectLayerByLocation_management("HUC48_lyr", "INTERSECT", spec_location)
        arcpy.MakeFeatureLayer_management("HUC48_lyr", "slt_lyr")
        rows = arcpy.da.SearchCursor("slt_lyr", HUC2Field)
        HUC2set = set()
        HUC2list = []
        for row in rows:
            t = str(row[0])
            HUC2list.append(t)

        HUC2set.update(HUC2list)
        print HUC2set
        HUC2list = list(HUC2set)
        specieHUCdict[id] = HUC2list
        del HUC2list, HUC2set


    arcpy.Delete_management( "slt_lyr")
    arcpy.Delete_management("HUC48_lyr")


datelist = []
today = datetime.date.today()
datelist.append(today)

specieHUCdict = {}
HUCdict = {}
for k, v in globals().items():
    if k.startswith('huc'):
        num = k.replace("huc", "")
        HUCdict[num] = v

start = time.time()
start_times(start)
LYR_dir=r'J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\temp'
for fc in StreamCrosswalk.functions.fcs_in_workspace(MasterSpeFC):
    checkprj = fc.split('_')
    if 'AlbersUSA' == checkprj[4]:
        continue
    if 'PartialLower48' != checkprj[0]:
        continue
    print fc
    print specieHUCdict
    inputfc = MasterSpeFC +os.sep +fc
    streamcrosswalk(inputfc, HUC2_lwr48,LYR_dir,  specieHUCdict,DDspecies)

listHUCs = specieHUCdict.keys()
results = []

for value in listHUCs:
    entid = value
    HUCSs = specieHUCdict[value]
    for huc in HUCSs:
        listresult = [entid, huc, fileType]
        print listresult
        results.append(listresult)

outDF = pd.DataFrame(results)
# print outDF

outpath = outlocation + os.sep + outfile
outDF.to_csv(outpath)

done = time.time()
end_times(done, start)
