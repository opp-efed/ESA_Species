import os
import pandas as pd
import datetime

import arcpy

# Title - Check projection of all use to make sure all projections accounted for in steps 7
InGDB = r'L:\Workspace\StreamLine\ByProjection\AK_UseLayers.gdb'
NameCSV = 'AK_Uses'
outCSVLocation = 'L:\Workspace\StreamLine\ByProjection'


# ####################FUNCTIONS
# recursively checks workspaces found within the inFileLocation and makes list of all rasters
def rasters_in_workspace(workspace):
    path = workspace
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        out_path = str(path) + os.sep + str(raster)
        yield (raster), out_path
    for ws in arcpy.ListWorkspaces():
        path = str(path) + os.sep + str(ws)
        for raster, out_path in rasters_in_workspace(ws):
            yield raster, out_path


# #####################################################################################################################

today = datetime.date.today()
date = today.strftime('%Y%m%d')

df_out_table = pd.DataFrame(
    columns=['FullName', 'Path', 'Region', 'Use', 'Type', 'Cell Size', "Projection", "Projection Type", 'Included AA',
             'FinalUseHeader', 'FinalColHeader', 'Action Area', 'ground', 'aerial', 'other layer'])

regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI','CDL', 'cultmask', 'OnOff']
chemicals = ['carbaryl', 'chlorpyrifos', 'diazinon', 'malathion', 'methomyl']
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

count = 0
for raster, r_path in rasters_in_workspace(InGDB):
    # # get cell size
    CELLSIZEXResult = arcpy.GetRasterProperties_management(r_path, "CELLSIZEX")
    CELLSIZEYResult = arcpy.GetRasterProperties_management(r_path, "CELLSIZEY")
    cell_size = CELLSIZEXResult.getOutput(0), CELLSIZEYResult.getOutput(0)
    # # Get chemical
    chemical = []
    split_path = r_path.split(os.sep)
    for v in split_path:
        for i in v.split("_"):
            if i in chemicals and i not in chemical:
                chemical.append(i)
    # get region
    if 'CONUS' in r_path.split(os.sep):
        region = "CONUS"
    else:
        try:
            region = [v for v in raster.split("_") if v in regions][0]
            if region == 'CDL' or region == 'cultmask'or region == 'OnOff':
                region = 'CONUS'
        except:
            pass

    # extracts spatial info for each raster
    ORGdsc = arcpy.Describe(r_path)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    df_out_table = df_out_table.append(
        {'FullName': raster, 'Path': r_path, 'Region': region, 'Type': 'Raster', 'Cell Size': cell_size,
         "Projection": ORGsr.name, "Projection Type": ORGsr.type, 'Included AA': chemical}, ignore_index=True)

    print "{0}, {1}, {2}, {3} ".format(raster, ORGsr.name, cell_size, chemical)
    # df_out_table = df_out_table.append(
    #     {'FullName': raster, 'Path': r_path, 'Region': region, 'Type': 'Raster',
    #      "Projection": ORGsr.name, "Projection Type": ORGsr.type, 'Included AA': chemical}, ignore_index=True)
    #
    # print "{0}, {1}, {2} ".format(raster, ORGsr.name, chemical)

    # df_out_table = df_out_table.append(
    #     {'FullName': raster, 'Path': r_path, 'Region': region, 'Type': 'Raster','Included AA': chemical},
    #     ignore_index=True)
    #
    # print "{0}, {1} ".format(raster, chemical)
df_out_table.to_csv(outCSVLocation + os.sep + NameCSV + "_" + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
