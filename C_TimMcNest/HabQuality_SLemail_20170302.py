import arcpy, datetime
from arcpy import env
from arcpy.sa import *
arcpy.CheckOutExtension("Spatial")

ws = "C:/Users/slennart/Documents/CDL/PopModel/CAGN/CAGN.gdb"
arcpy.env.workspace = ws
arcpy.env.scratchWorkspace = ws
arcpy.env.overwriteOutput = 1
arcpy.env.parallelProcessingFactor = "100%"

# Coastal Sage Scrub (CSS), prepared using suitable habitats from USGS GAP. Coastal habitats are weighted 1 (30m cell
#  is 900m2. Inland habitats are weighted 0.67 (30m cell is 603m2). The raster is then smoothed using a 7x7 focal
# mean, followed by zeroing out non-habitat cells.
css = Raster("CA_GAP_v221_SuitHab_wghtd_7x7b")
# Distance to Trees (DTR). Prepared using USGS GAP, NVC_CLASS "Forest & Woodland", applying euclidean distance.
dtr = Raster("CA_GAP_Forest_euc_intx")
# Distance to Grassland (DTG). Prepared using USGS GAP, ECOLSYS_LU's with grassland, prairie, or meadow,
# applying euclidean distance.
dgr = Raster("CA_GAP_Grassland_euc_intx")
# California 30m spatial resolution DEM
dem = Raster("CA_DEM")

arcpy.env.snapRaster = dem
arcpy.env.extent = dem

# Today's date
today = datetime.datetime.today().strftime('%y%m%d')
print("Date: {}".format(today))

# Linear combination of variables (X)
t1 = datetime.datetime.now()
print("Calculating X at {}".format(t1))
x = 1.8223 + 6.7494 * 10**-4 * css - 8.9122 * 10**-3 * dem - 2.6755 * 10**-3 * dgr - 1.8863 * 10**-3 * dtr - 18.3337 * 10**-7 * css * dem + 4.2860 * 10**-7 * css * dtr + 8.5897 * 10**-6 * dem * dtr
print("Calculated X in {}".format(datetime.datetime.now() - t1))

# Conversion to habitat quality Y, range: (0-1) using the logit transformation
t2 = datetime.datetime.now()
print("Calculating Y at {}".format(t2))
y = (math.e**x) / (1 + math.e**x)
print("Calculated Y in {}".format(datetime.datetime.now() - t2))

t3 = datetime.datetime.now()
print("Saving Y at {}".format(t3))
out = "{}/Y_{}".format(ws, today)
y.save(out)
print("Saved Y in {}".format(datetime.datetime.now() - t3))

print("Building Pyramids")
arcpy.BuildPyramids_management(out)

print("Completed Script in {}".format(datetime.datetime.now() - t1))
