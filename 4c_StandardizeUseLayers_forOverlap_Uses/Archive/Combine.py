import arcpy
from arcpy.sa import *
import datetime

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
arcpy.CheckOutExtension("Spatial")


where = '"VALUE" = 0'
arcpy.MakeRasterLayer_management("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive"
                                 "\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb"
                                 "\Albers_Conical_Equal_Area_CONUS_CDL_1015_60x2_euc", "rdlayer_60", where)

arcpy.MakeRasterLayer_management("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive"
                                 "\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb"
                                 "\Albers_Conical_Equal_Area_CONUS_CDL_1015_70x2_euc", "rdlayer_70", where)

arcpy.MakeRasterLayer_management("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive"
                                 "\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb"
                                 "\Albers_Conical_Equal_Area_CONUS_Nurseries_euc", "rdlayer_nurse", where)

AAoutCombine = Combine(["rdlayer_60", "rdlayer_70","rdlayer_nurse"])
AAoutCombine.save("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA"
                  "\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb\CONUS_AA_Diaz")

# outCombine.save("C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ExternalDrive\_UseSites\CONUS_Diaz_UseLayer.gdb\CONUS_Ag_Diaz")
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)