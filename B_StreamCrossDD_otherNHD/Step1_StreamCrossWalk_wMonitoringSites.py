import os
import datetime
import arcpy
import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title- Identifies stream that cross the boundary of a species location files, monitoring sites within user defined
# buffer distance and near tables for species to monitoring sites
#           1) Species merged composite files for Range and CH
#           2) Species of concern - from Colleen or Chuck - aquatic species with and NE or NLAA call
#           4) Streams file with COMID from NHDPlus
#           5) Monitoring sites for each chemical- from Chuck

# Assumptions
#   1) EntityID must be in species composite files with "EntityID"
#   2) COMID must be in the NHD file- stream_lines with header "COMID"
#   3) CROSSED_BY_THE_OUTLINE_OF - selection method for steam can be change on line 121
#   3) Monitoring site point file must have headers found in line 234 see columns=
#   4) Near table output must contain headers in line 202 see columns=
#   5) If error causes script to stop; it can be restarted but any partial files generated prior to error must be
#       deleted


# NOTE:  if the script is stopped and a table was started but not completed for a species it must be deleted before
# Starting the script again.  Created table will cause the species to be skipped when the script is re-started be sure
# all saved tables include finished tables

# ## INPUTS
# out location
ws = "C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\GIS Data\Downstream Transport\DT_Crosswalks_Buffers"
name_dir = 'Range'  # change to CriticalHabitat for CH runs and Range for Range runs

# GDB with all species locations composites to run- can be the range or critical habitat
MasterSpeFC = r"E:\Workspace\StreamLine\ESA\CompositeFiles_Spring2020\R_SpGroupComposite.gdb"
# Master species list with updated Downstream Transport Species column see script DownStream_SpeAssignment;
# the only values in the Downstream Transport Species should be Yes and No - confirm there are no blanks
master_list = "C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
# buffer distance from the species location to identify the monitoring sites
buffer_distance = "67 Miles"
# Monitoring site shape files - chemical name must be the first part of the file name followed by an underscore
# Check the column headers used in the monitoring sites - this are hard codes in the script as:
#            'Site_ID', 'Latitude', 'Longitude'
monitoring_points = ["C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\GIS Data\Downstream Transport\Glyphosate\GLY_XYMonitoring_Sites_for_Downstream_Transport_Analysis.shp"]
# Stream files
stream_lines = "F:\AquaticTables_ESA\Spring 2018\NHDPlusV21_National_Seamless.gdb\NHDSnapshot\NHDFlowline_Network"

# Static variables
# List of species to consider
# Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
# set entityid as string for merges
species_df ['EntityID'] = species_df ['EntityID'].map(lambda t: t.split(".")[0]).astype(str)
species_filtered = species_df[species_df['Downstream Transport Species'] == 'Yes']
species_to_run = species_filtered['EntityID'].values.tolist()
print 'Species being considered for downstream transport {0}'.format(species_to_run)

# ## OUTPUTS
# File type flag for species to use in output file names - R or CH from MasterSpeFC; and date
path, gdb = os.path.split(MasterSpeFC)
file_type = gdb.split('_')[0]
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# output file names
no_nhd_csv = "AquSpe_" + str(file_type) + "_NoStream_" + date + ".csv"  # species- no streams in location file
no_points_csv = "AquSpe_" + str(file_type) + "_NoMonitoring_" + date + ".csv"  # species- no monitoring within buffer
all_spe_stream_csv = "AquSpe_" + str(file_type) + "_AllStreams_" + date + ".csv"  # all species and streams
all_spe_monitoring_csv = "AquSpe_" + str(file_type) + "_AllMonitoring_" + date + ".csv"  # all species and monitoring
all_spe_near_csv = "AquSpe_" + str(file_type) + "_AllNearTables_" + date + ".csv"  # all species near tables
out_gdb_name = "Buffer_67Miles.gdb"  # output gdb for buffered location files


# FUNCTIONS
def create_directory(dbf_dir):
    # creates directories if they don't exist
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        print "created directory {0}".format(dbf_dir)


def fcs_in_workspace(workspace):
    # loops over a workspace and recursively looks for all feature classes - returns the feature class
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for wks in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(wks):
            yield fc


def stream_crosswalk(in_fc, streams_files, lyr_dir, no_nhd, csv_path, buffer_dis, no_point, out_gdb, out_monitoring):
    # For each row/species the streams are identified using the selected by location then exported to a csv; if no
    # streams are within the species location the Entity ID is added to the no nhd file
    # Output- all streams across all HUC2s by species, one master table for all species and associated streams, list of
    # species with no nhd streams
    arcpy.Delete_management("stream_lyr")  # deletes old file in memory if it exists
    arcpy.MakeFeatureLayer_management(streams_files, "stream_lyr")  # generate memory layer of streams
    # if the master file with all species and streams has been generated from a previous features class reads in the
    # results, if no master file exists generates an empty df to store results for all species
    if os.path.exists(csv_path + os.sep + all_spe_stream_csv):
        all_species_streams = pd.read_csv(csv_path + os.sep + all_spe_stream_csv)
        [all_species_streams.drop(p, axis=1, inplace=True) for p in all_species_streams.columns.values.tolist() if
         p.startswith('Unnamed')]
    else:
        all_species_streams = pd.DataFrame(columns=['COMID', 'EntityID'])
    # search cursor over the attribute table - looks at each species
    for row in arcpy.SearchCursor(in_fc):
        spe_time_start = datetime.datetime.now()  # clock to track time per species
        lyr = file_type + "_{0}_lyr".format(row.EntityID)  # layer name for selected features
        out_layer = lyr_dir + os.sep + lyr + ".lyr"  # path to save layer files
        # NOTE - Column heading in attribute table must be EntityID- HARD CODE
        table = "{1}_Streams_{0}.csv".format(row.EntityID, file_type)  # File name with species EntityID
        # NOTE - Column heading in attribute table must be EntityID - HARD CODE
        entid = "{0}".format(row.EntityID)  # Extract entity ID number from attribute tables
        if entid not in species_to_run:  # skips species not in species_to_run list
            continue
        elif not arcpy.Exists(out_layer):  # generates output species layer file if it doesn't exist
            where_clause = "EntityID = '%s'" % entid  # Where clause to select species location file for the one species
            print (where_clause)  # prints where clause
            # Makes a feature layer that will only include current entid using where_clause
            arcpy.MakeFeatureLayer_management(in_fc, lyr, where_clause)
            print "Creating layer {0}".format(lyr)  # prints for  visual tracking of progress
            arcpy.SaveToLayerFile_management(lyr, out_layer, "ABSOLUTE")  # saves layer of species range to be used
            print "Saved layer file"  # prints for  visual tracking of progress

        spec_location = str(out_layer)  # path to layer file that was saved
        # select the streams that CROSSED_BY_THE_OUTLINE_OF the species location file
        # changed from INTERSECT to CROSSED_BY_THE_OUTLINE_OF because we only need the streams that cross the boundary
        # of the species file to determine if a monitoring point is up or down stream; change to INTERSECT if all
        # streams in the species files are needed
        if not os.path.exists(csv_path + os.sep + table):  # check if stream file exists if not generates
            arcpy.SelectLayerByLocation_management("stream_lyr", "CROSSED_BY_THE_OUTLINE_OF", spec_location, "",
                                                   "NEW_SELECTION")
            count_selected = arcpy.GetCount_management("stream_lyr")  # get count of selected features
            arcpy.CopyRows_management("stream_lyr", csv_path + os.sep + table)  # saves selected streams to csv
        else:
            count_selected = "n/a"  # if stream file previously generated set parameter to n/a
        df_stream = pd.read_csv(csv_path + os.sep + table)  # reads stream csv into a df
        if len(df_stream) > 0:
            # NOTE- COMID must be in stream files - HARD CODE
            df_stream = df_stream[['COMID']]  # extracts just the COMID from the stream table
            df_stream['EntityID'] = entid  # add entity ID to the stream file
            # visual QC that the number of selected stream match the csv
            print ('   {0} Number of features in saved file; {1} number of features selected'.format(len(df_stream),
                                                                                                     count_selected))
            # Output locations
            print ('   Final file can be found at {0}'.format(csv_path + os.sep + table))  # print for tracking
            print ("   Completed in {0}\n".format((datetime.datetime.now() - spe_time_start)))  # elapse time print
            # appends current species stream to all species stream file
            all_species_streams = pd.concat([all_species_streams, df_stream], axis=0)
            del df_stream  # deletes df
        else:  # if there are no streams selected for a species it is added to the no_nhd files
            filename = str(lyr)  # gets the lyr name for the species
            print lyr
            print filename
            if filename not in no_nhd:  # if lyr not already in the no_nhd list appends it
                no_nhd.append(filename)
        # executes buffer on species file to look for the monitoring site
        no_point = buffer_files(spec_location, out_gdb, table, monitoring_points, no_point, buffer_dis,
                                out_monitoring, entid, in_fc)

    no_nhd_df = pd.DataFrame(data=no_nhd,
                             columns=['Species file'])  # takes the species with no streams and saves to a df
    no_nhd_df['Comment'] = 'No streams within species file'  # adds comment for track
    no_nhd_df.to_csv(csv_path + os.sep + no_nhd_csv)  # exports the no nhd streams species

    all_species_streams.drop_duplicates(inplace=True)  # drop duplicate values
    all_species_streams.to_csv(csv_path + os.sep + all_spe_stream_csv)  # exports working all species all streams file
    return no_nhd, no_point


def buffer_files(spe_lyr, out_gdb, out_filenm, monitoring_pnts, no_point, buffer_dis, out_monitoring, ent_id, in_fc):
    # Buffers the species location file and search for monitoring site with the extent that include the species
    # location file and the buffer; distance of buffer is based on user input. The monitoring points found
    # within the species file + buffer distance are are identified using the selected by location and exported to a
    # csv; if no monitoring points are within the species file + buffer distance the species Entity ID is added to the
    # no point file.  A near table is also generated for each species composite file to identify the closest
    # monitoring point for each species; for species with no monitoring point this identifies the closest one.
    # Output- monitoring sites with the species+ buffer distance by species (one per chemical),one master table for all
    # species for each monitoring site input file (by chemical), list of species with no monitoring sites and a near
    # table  identifying the closest monitoring site for all species files by chemical

    time_start = datetime.datetime.now()  # time tracker for each species
    # file names to be used for outputs
    out_filenm_buffer = (out_filenm.replace('Streams', ("Buffer_" + buffer_dis))).replace(" ", "_")
    out_filenm_buffer = out_filenm_buffer.replace('.csv', "")
    outfile = out_gdb + os.sep + out_filenm_buffer  # path to final buffered species files

    # Loads attribute table from species file into a df so species attributes can be merged to near table output
    list_fields = [f.name for f in arcpy.ListFields(in_fc)]  # get list of fields from input fc
    # Geometry fields start with the word shape and add dimensions to the array; don't need to include these fields
    list_fields = [p for p in list_fields if not p.startswith("Shape")]
    att_df = pd.DataFrame(arcpy.da.TableToNumPyArray(in_fc, list_fields))  # exports att table to df
    att_df['EntityID'] = att_df['EntityID'].map(lambda (n): n).astype(str)  # sets entity id as a str
    del list_fields  # deletes field list

    if not arcpy.Exists(outfile):  # checks if species already has a buffered file saved
        # apply buffer based on user input - it will be in all directions and round
        # this can be changed to apply a search distance to select by location if the buffer file is not needed
        arcpy.Buffer_analysis(spe_lyr, outfile, buffer_dis, "FULL", "ROUND", "LIST")
        print ("Buffer file for species {0} file can be found at {1}".format(ent_id, outfile))
    arcpy.MakeFeatureLayer_management(outfile, "buffer_lyr")  # make a layer file of the buffered species location

    for v in monitoring_pnts:  # loops over monitoring input files, typically one per chemical of interest
        chemical = os.path.basename(v).split("_")[0]  # extracts chemical name from point file; must be in position 0
        out_table = os.path.basename(in_fc)  # extract the out table name from the feature class name

        # if the master file with all species and the nearest monitoring sites has been generated from a previous
        # features class reads in the results, if no master file exists, generates an empty df to store results
        if os.path.exists(out_monitoring + os.sep + chemical.capitalize() + "_" + all_spe_near_csv):
            all_near_sites = pd.read_csv(out_monitoring + os.sep + chemical.capitalize() + "_" + all_spe_near_csv)
            # drops previous index columns that start with Unnamed
            [all_near_sites.drop(p, axis=1, inplace=True) for p in all_near_sites.columns.values.tolist() if
             p.startswith('Unnamed')]
        else:
            all_near_sites = pd.DataFrame(columns=['OBJECTID', 'IN_FID', 'NEAR_FID', 'NEAR_DIST', 'FROM_X', 'FROM_Y',
                                                   'NEAR_X', 'NEAR_Y'])
        # checks if the near table for the species composite file and chemical already exists; if not it is generated
        # near tables uses the GEODESIC distances and will be in meters but may not be exactly the same as the planar
        # distances used for euclidean distances estimates when buffering; GEODESIC is the recommenced parameter when
        # using geographic coordinate systems or those not optimized for distance calculations.
        # https://pro.arcgis.com/en/pro-app/tool-reference/analysis/generate-near-table.htm
        if not os.path.exists(out_monitoring + os.sep + out_table + "_NearTable_" + chemical + '.csv'):
            arcpy.GenerateNearTable_analysis(in_fc, v, (
                    out_monitoring + os.sep + os.path.basename(in_fc) + "_NearTable_" + chemical + '.csv'), "#",
                                             "LOCATION", "#", "CLOSEST", "#", "GEODESIC")
        # read near table into df
        near_df = pd.read_csv(out_monitoring + os.sep + os.path.basename(in_fc) + "_NearTable_" + chemical + '.csv')
        # merges the near table results with the species attribute based on the keys used for the near table
        near_df = pd.merge(near_df, att_df, how='left', left_on='IN_FID', right_on='OBJECTID')
        # filters result table to include just the species found on the species_to_run list
        near_df = near_df[near_df['EntityID'].isin(species_to_run)]
        # near table distance based on GEODESIC will be in meter converts this value to miles
        near_df['NEAR_DIST_MILES'] = near_df['NEAR_DIST'].multiply(0.000621371, axis=0)
        all_species_near = pd.concat([all_near_sites, near_df], axis=0)  # concatenates to master table
        all_species_near.drop_duplicates(inplace=True)
        # exports working species/all near sites
        all_species_near.to_csv(out_monitoring + os.sep + chemical.capitalize() + "_" + all_spe_near_csv)
        # checks if the master file for a species/monitoring points has been generated, if no file exists, generates
        # an empty df to store results
        if os.path.exists(out_monitoring + os.sep + chemical.capitalize() + "_" + all_spe_monitoring_csv):
            all_monitoring_sites = pd.read_csv(out_monitoring + os.sep + chemical.capitalize() + "_" +
                                               all_spe_monitoring_csv)
            # drops previous index columns that start with Unnamed
            [all_monitoring_sites.drop(p, axis=1, inplace=True) for p in all_monitoring_sites.columns.values.tolist()
             if p.startswith('Unnamed')]
        else:
            all_monitoring_sites = pd.DataFrame(columns=['Site_ID', 'Latitude', 'Longitude', 'EntityID'])
        # Set up the files names for the output csvs with the monitoring points
        out_filenm_point = out_filenm.replace('Streams', ("Monitoring Site_" + chemical))
        arcpy.Delete_management("point_lyr")  # deletes old monitoring point file in memory if it exists
        arcpy.MakeFeatureLayer_management(v, "point_lyr")  # generate layer for current monitoring point file
        # select the monitoring points that intersect the species location file
        if not os.path.exists(out_monitoring + os.sep + out_filenm_point):  # check if table exist
            arcpy.SelectLayerByLocation_management("point_lyr", "INTERSECT", "buffer_lyr", "", "NEW_SELECTION")
            count_selected = arcpy.GetCount_management("point_lyr")  # get count of selected features
            # saves to selected monitoring points that are within the buffer file to a csv
            arcpy.CopyRows_management("point_lyr", out_monitoring + os.sep + out_filenm_point)
        else:
            count_selected = 'n/a'  # if stream file previously generated set parameter to n/a
        df_monitoring = pd.read_csv(out_monitoring + os.sep + out_filenm_point)  # reads csv into a df
        if len(df_monitoring) > 0:
            # NOTE- Monitoring, lat and long  must be in stream files - HARD CODE
            # extracts the monitoring info from saved table
            df_monitoring = df_monitoring[['Site_ID', 'Latitude', 'Longitude']]
            df_monitoring['EntityID'] = ent_id  # add entity ID to the stream file
            # visual QC that the number of selected stream match the csv
            print ('   {0} Number of features in saved file; {1} number of features selected'.format(len(df_monitoring),
                                                                                                     count_selected))
            print ('   Final file can be found at {0}'.format(out_monitoring + os.sep + out_filenm_point))  # tracking
            print ("   Completed in {0}\n".format((datetime.datetime.now() - time_start)))  # elapse time print
            # appends current species stream to all species stream file
            all_monitoring_sites = pd.concat([all_monitoring_sites, df_monitoring], axis=0)
            del df_monitoring  # deletes df
            print ('   Final file can be found at {0}'.format(
                out_monitoring + os.sep + out_filenm_point))  # print tracking
            print ("   Completed in {0}\n".format((datetime.datetime.now() - time_start)))  # elapse time print
            all_monitoring_sites.to_csv(out_monitoring + os.sep + chemical.capitalize() + "_" + all_spe_monitoring_csv)
        else:
            filename = out_filenm_buffer
            if filename not in no_point:
                no_point.append(filename)
        # convert list of species with no monitoring points to a df
        df_no_points = pd.DataFrame(data=no_point, columns=['Entity'])
        df_no_points['Comment'] = 'No Monitoring site within {0}'.format(buffer_dis)
        df_no_points.drop_duplicates(inplace=True)  # drops any duplicate records
        # checks if output file has been create if so appends the new information, if not creates output file
        # saves the species with no point within the buffer distance to a csv
        if not os.path.exists(out_monitoring + os.sep + chemical.capitalize() + "_" + no_points_csv):
            df_no_points.to_csv(out_monitoring + os.sep + chemical.capitalize() + "_" + no_points_csv)
        else:
            all_sp_no_points = pd.read_csv(out_monitoring + os.sep + chemical.capitalize() + "_" + no_points_csv)
            # drops previous index columns that start with Unnamed
            [all_sp_no_points.drop(p, axis=1, inplace=True) for p in all_sp_no_points.columns.values.tolist()
             if p.startswith('Unnamed')]
            all_sp_no_points = pd.concat([all_sp_no_points, df_no_points], axis=0)
            all_sp_no_points.to_csv(out_monitoring + os.sep + chemical.capitalize() + "_" + no_points_csv)
    arcpy.Delete_management("buffer_lyr")  # deletes buffered species layer from memory
    return no_point


def main():
    start = datetime.datetime.now()
    print "Start Time: " + start.ctime()

    # workspace and output paths
    file_dir = ws + os.sep + str(name_dir)
    out_path_buffer_gdb = file_dir + os.sep + out_gdb_name
    lyr_dir = file_dir + os.sep + 'LYR'
    csv_dir = file_dir + os.sep + 'Streams'
    monitoring_dir = file_dir + os.sep + 'Monitoring Sites'
    csv_path = csv_dir

    no_nhd = []  # empty list to hold species without any streams; this is exported to a csv at end
    no_points = []  # empty list to hold species without any monitoring points; this is exported to a csv at end

    # Create output locations
    create_directory(file_dir)
    create_directory(lyr_dir)
    create_directory(csv_dir)
    create_directory(monitoring_dir)
    if not arcpy.Exists(out_path_buffer_gdb):
        arcpy.CreateFileGDB_management(file_dir, out_gdb_name)

    arcpy.env.workspace = MasterSpeFC
    fc_list = arcpy.ListFeatureClasses()
    print fc_list

    # Execute loop on each merged species composite file for each species in each composite file checks if the species
    # should be included, then extract streams, monitoring sites and near tables
    for fc in fc_list:
        print fc
        input_fc = MasterSpeFC + os.sep + fc
        no_nhd, no_points = stream_crosswalk(input_fc, stream_lines, lyr_dir, no_nhd, csv_path,
                                             buffer_distance, no_points, out_path_buffer_gdb, monitoring_dir)

    print no_nhd
    print no_points

    end = datetime.datetime.now()
    print "End Time: " + end.ctime()
    elapsed = end - start
    print "Elapsed  Time: " + str(elapsed)


main()
