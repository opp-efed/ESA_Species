import os
import pandas as pd
import datetime
from arcpy.sa import *
import arcpy

# Title - Re-projects union raster into projection by region
# in and out location
in_table = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
           r'\_ExternalDrive\_CurrentSupportingTables\UseLayer_Tables\UseLayers_20180103.csv'

midGDB = r'L:\Workspace\UseSites\scratch.gdb'

# projection folder - check all values in Final Projection columns have a corresponding file with the same name
prjFolder = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ExternalDrive\projections\FinalBE'

# snap raster must be in desired projection with the desired cell size
#run_regions = ['CONUS']
# run_regions = ['AK', ]
run_regions = ['HI','AS', 'CNMI', 'GU',  'PR', 'VI']

SnapRaster_Dict = {
    'CONUS': r'L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2016',
    'HI': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\AA_Ag\methomyl_HI_171227.gdb\HI_Ag',
    'AK': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\AA_Ag\methomyl_AK_171227.gdb\AK_Ag',
    'AS': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\AA_Ag\methomyl_AS_171227.gdb\AS_Ag',
    'CNMI': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\AA_Ag\methomyl_CNMI_171227.gdb\CNMI_Ag',
    'GU': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\Resample\AA\methomyl_GU_171227.gdb\GU_Ag_30',
    'PR': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\AA_Ag\methomyl_PR_171227.gdb\PR_Ag',
    'VI': r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Lennartz, Steven - Chemicals\NL48\Resample\AA\methomyl_VI_171227.gdb\VI_Ag_30'}


def create_gdb(out_folder, out_name, outpath):  # Create a new GDB
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(inraster, prj_file, prj_folder, region_dict, region, outraster, cellsize):
    start_raster = datetime.datetime.now()

    in_raster = Raster(inraster)
    # extract snap raster for region

    # snap_raster = Raster(region_dict[region])
    # arcpy.Delete_management("snap")
    # arcpy.MakeRasterLayer_management(snap_raster, "snap")
    # arcpy.env.snapRaster = "snap"

    # location prj files
    wgs_coord_file = prj_folder + os.sep + 'WGS_1984.prj'
    nad83_coord_file = prj_folder + os.sep + 'NAD_1983.prj'
    prj_file_path = prj_folder + os.sep + prj_file + '.prj'

    current_raster_dsc = arcpy.Describe(in_raster)
    current_sr = current_raster_dsc.spatialReference
    current_datum = current_sr.GCS.datumName
    ORGprj = current_sr.name

    # extract spatial information from prj files
    dsc_wgs = arcpy.Describe(wgs_coord_file)
    wgs_coord_sys = dsc_wgs.spatialReference

    dsc_nad83 = arcpy.Describe(nad83_coord_file)
    nad83_coord_sys = dsc_nad83.spatialReference

    dsc_prj = arcpy.Describe(prj_file_path)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName
    prj_name = prj_file.replace('.prj', '')
    OUTprj = prj_sr.name

    print ORGprj, OUTprj

    # No re-projection - raster is copied and file name is updated
    if ORGprj == OUTprj:
        if not arcpy.Exists(outraster):
            print 'Copying {0}'.format(inraster)
            print inraster,outraster
            arcpy.CopyRaster_management(inraster, outraster)
        else:
            print str(outraster) + " already exists"
    # Check Datum then re-projects

    elif prj_datum == "D_WGS_1984":
        if current_datum != "D_WGS_1984":
            raster_other_geo = str(os.path.basename(inraster)) + "_WGS84"
            out_other_raster = midGDB + os.sep + raster_other_geo
            if not arcpy.Exists(out_other_raster):
                print 'Projecting {0} into {1} from '.format(inraster, 'WGS 1984', 'NEAREST', cellsize)
                arcpy.ProjectRaster_management(in_raster, out_other_raster, wgs_coord_sys)
            if not arcpy.Exists(outraster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(out_other_raster, outraster, prj_sr, 'NEAREST', cellsize)
            else:
                print str(outraster) + " already exists"
        else:
            if not arcpy.Exists(outraster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(in_raster, outraster, prj_sr, 'NEAREST', cellsize)
            else:
                print str(outraster) + " already exists"

    else:
        if prj_datum == "D_North_American_1983":
            if current_datum != "D_North_American_1983":

                raster_other_geo = str(os.path.basename(inraster)) + "_NAD83"
                out_other_raster = midGDB + os.sep + raster_other_geo

                if not arcpy.Exists(out_other_raster):
                    print 'Projecting {0} into {1}'.format(inraster, 'NAD83')
                    arcpy.ProjectRaster_management(in_raster, out_other_raster, nad83_coord_sys)

                if not arcpy.Exists(outraster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(out_other_raster, outraster, prj_sr, 'NEAREST', cellsize)
                else:
                    print str(outraster) + " already exists"
            else:
                if not arcpy.Exists(outraster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(in_raster, outraster, prj_sr, 'NEAREST', cellsize)
                else:
                    print str(outraster) + " already exists"

    print 'Completed projection in: {0}\n'.format((datetime.datetime.now() - start_raster))


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if not os.path.exists(midGDB):
    create_gdb(os.path.dirname(midGDB), os.path.basename(midGDB), midGDB)
in_df = pd.read_csv(in_table)
in_df = in_df.loc[in_df['Region'].isin(run_regions)]
list_raster = in_df['Current Path'].values.tolist()

for v in list_raster:
    out_raster = in_df.loc[in_df['Current Path'] == v, 'Final Path'].iloc[0]
    path, tail = os.path.split(out_raster)
    if not os.path.exists(path):
        create_gdb(os.path.dirname(path), os.path.basename(path), path)
    if out_raster != 'None':
        region_abb = in_df.loc[in_df['Current Path'] == v, 'Region'].iloc[0]
        prj_nm = in_df.loc[in_df['Current Path'] == v, 'Final Projection'].iloc[0]
        cell_size = in_df.loc[in_df['Current Path'] == v, 'Cell Size'].iloc[0].split(',')[0].replace("(u", "")

        try:
            raster_project(v, prj_nm, prjFolder, SnapRaster_Dict, region_abb, out_raster, cell_size)
        except Exception as error:
            print(error.args[0])

end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
