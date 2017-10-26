import os

StreamMilescsv = 'C:\Workspace\DD_Species_StreamCross_UpdateMarch2016\StreamMilesHUCDDOnly_updatedStreamsR.csv'
AquaticPost = 'C:\Workspace\DD_Species_StreamCross_UpdateMarch2016\species hic bin assignments_201603010.csv'

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
             '10298', '10299', '10300', '10301', '10517', '10910', 'NMFS166', 'NMFS88', '286']

streamMilesdict = {}
with open(StreamMilescsv, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        ent = line[1]
        if ent not in DDspecies:
            continue
        huc = line[3]
        try:
            huclist = streamMilesdict[ent]
            huclist.append(huc)
            huclist.sort()
            streamMilesdict[ent] = huclist
        except:
            streamMilesdict[ent] = [huc]
print streamMilesdict

inputFile.close()

APdict = {}
with open(AquaticPost, 'rU') as inputFile2:
    header = next(inputFile2)
    for line in inputFile2:
        line = line.split(',')
        #print line

        ent = str((line[1]))
        ent = ent.replace('\n', '')
        if ent not in DDspecies:
            continue
        huc = line[0]
        try:
            huclist = APdict[ent]
            huclist.append(huc)
            huclist.sort()
            APdict[ent] = huclist
        except:
            APdict[ent] = [huc]
print APdict
inputFile.close()

Apent = APdict.keys()
mileent = streamMilesdict.keys()

onlyap = []
apHUCfail = []
onlymiles = []
smHucfail = []

for v in Apent:
    if v not in mileent:
        onlyap.append(v)
        continue
    else:
        aphuc = APdict[v]
        aphuc.sort()
        smhuc = streamMilesdict[v]
        smhuc.sort()
        for value in aphuc:
            if value not in smhuc:
                if value not in apHUCfail:
                    apHUCfail.append(v)
            else:
                continue

for v in mileent:
    if v not in Apent:
        onlymiles.append(v)
    else:
        aphuc = APdict[v]
        aphuc.sort()
        smhuc = streamMilesdict[v]
        smhuc.sort()
        for value in aphuc:
            if value not in smhuc:
                if value not in smHucfail:
                    smHucfail.append(v)
            else:
                continue
missingAP = []
missingmiles =[]
for species in DDspecies:
    if species not in Apent:
        missingAP.append(species)

for species in DDspecies:
    if species not in mileent:
        missingmiles.append(species)


print "Species only in Post procest {0}".format(onlyap)
print "Species only in StreamMiles {0}".format(onlymiles)

print "Species with differt hucs in Post procest {0}".format(set(apHUCfail))
print "Species with different huc StreamMiles {0}".format(set(smHucfail))

print "DD Species missing from in Post procest {0}".format(missingAP)
print "DD Species missing from StreamMiles {0}".format(missingmiles)
