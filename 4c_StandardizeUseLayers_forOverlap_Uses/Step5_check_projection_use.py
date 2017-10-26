import os
import csv
import pandas as pd
import datetime

import arcpy

# Title - Check projection of all use to make sure all projections accounted for in steps 7
InGDB = 'J:\Workspace\EDM_2015\Euclidean'
NameCSV = 'Euc_UseSites_projection'
outCSVLocation = 'J:\Workspace\EDM_2015\Euclidean'


# ####################FUNCTIONS
# recursively checks workspaces found within the inFileLocation and makes list of all rasters
def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster


# outputs table from list generated in create FileList
def create_out_table(list_name, csv_name):
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


def create_flnm_timestamp(namefile, outlocation, date_list, file_extension):
    file_extension.replace('.', '')
    filename = str(namefile) + "_" + str(date_list[0]) + '.' + file_extension
    filepath = os.path.join(outlocation, filename)
    return filename, filepath


# #####################################################################################################################

OrgSRList = []  # Empty list to store information about each of the feature classes will be exported to table

datelist = []
today = datetime.date.today()
datelist.append(today)

header = ["Filename", "Projection", "Projection Type"]  # Heading for tables

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

df_out_table = pd.DataFrame(columns=header)

count = 0
for raster in rasters_in_workspace(InGDB):
    # extracts spatial info for each raster
    ORGdsc = arcpy.Describe(raster)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    df_out_table = df_out_table.append({"Filename": raster, "Projection": ORGsr.name,
                                        "Projection Type": ORGsr.type}, ignore_index=True)
    print "{0}  {1}".format(raster, ORGsr.name)
    count += 1

csvfile, csvpath = create_flnm_timestamp(NameCSV, outCSVLocation, datelist, 'csv')
df_out_table.to_csv(csvpath)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
