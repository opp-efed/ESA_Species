import arcpy
import os
import datetime
import pandas as pd


# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Tile: Checks for missing range files - currently not set up to check for missing CH files

# NOTE  Make sure all commas are removed from master table before running this script to a find all and
# replace
# TODO Update cross check to pandas df so that the commas are no longer a problem

# User input variable
masterlist = r"L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\No Call Species\NoCall_MasterListESA_Dec2018_20190130.csv"

# Composite GDB generate in Step A not a folder - must be a GDB
infolder = r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\No Call Species\Composites_NoCall\CH_SpGroupComposite.gdb'
group_fc_index = 1 # location of the sp group in the file name for the composite base 0
# also need to set the hard code to the index number for the cols reference in loop species

# Confirm column names before running
group_colindex = 'Group'
entid_colindex = "EntityID"
not_considered_colindex = 'not_considered_BE_GIS'  # On master but not be considered in BE; mostly Qualitative species
dev_colindex = 'Range under development'  # species range is under development
ch_gis_colindex = "CH_GIS" # available GIS file; Some species do not have a GIS file;GIS files for Qual species archived

# #########Functions

# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


# for each specie group will compare the species in that group to the files in found in the species GDB in the spatial
# library noted by the infolder if a file is not present that it is appended to list and printed at the end
def loop_species(group, flag):
    print "\nWorking on {0}".format(group)
    masterlist_df = pd.read_csv(masterlist)
    masterlist_df[entid_colindex]= masterlist_df[entid_colindex].astype(str)
    if flag =='CH': # filters list to just species with CH when working with CH files
        df_group = masterlist_df.loc[(masterlist_df[group_colindex] == group) & (masterlist_df[ch_gis_colindex] == "Yes")].copy()

    else:
        # filters out range files w/0 GIS files = these are mostly qualitative species
        df_group = masterlist_df.loc[(masterlist_df[group_colindex] == group)& (masterlist_df[not_considered_colindex] == "No")]
    group_entlist = df_group[entid_colindex].values.tolist()
    print group_entlist
    return group_entlist


def check_composite (fc, master_entlist_group):
    entlist_fc =[]
    with arcpy.da.SearchCursor(fc, ['EntityID']) as cursor:
            for row in cursor:
                entid = str(row[0])
                if entid not in master_entlist_group:
                    print "FILE IN WRONG Composite {0}".format(entid)
                    continue
                else:
                    # list will include duplicate entitiyID if there are multiple rows for a species this is one of the
                    # things this script checks
                    entlist_fc.append(entid)
    return entlist_fc


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

flag_ch_r = os.path.basename(infolder).split("_")[0]

# generate a list of specie groups from master
masterlist_df = pd.read_csv(masterlist)
if flag_ch_r =='CH':
    # when working with CH filter - filters master list to just those with CH
    masterlist_df = masterlist_df.loc[masterlist_df[ch_gis_colindex] == "Yes"]
else:
    # when working with range filters out species without GIS file; most qualitative
    masterlist_df = masterlist_df.loc[masterlist_df[not_considered_colindex] == "No"]


alpha_group = sorted(list(set(masterlist_df['Group'].values.tolist())))
print alpha_group

arcpy.env.workspace = infolder
fclist = arcpy.ListFeatureClasses()
print fclist

for group in alpha_group:

    # list all species in current group, species flagged as not longer considered, species flagged as range under dev,
    # and species that we are using the county range for
    entlist = loop_species(group, flag_ch_r)
    group_fc = [fc for fc in fclist if fc.startswith(flag_ch_r+"_"+group.split(" ")[0])][0]
    species_in_comp = check_composite(infolder +os.sep+group_fc,entlist)

    missing_files = []
    extrafiles = []

    # check for species not included in composite
    for value in entlist:
        if str(value) not in species_in_comp:
            missing_files.append(str(value))
        else:
            continue

    # checks for species in composite but not on master
    for value in species_in_comp:
        if value not in entlist:
            extrafiles.append(value)
        else:
            continue

    # print feedback about species missing files or extra
    if len (missing_files) == 0 and len(extrafiles) == 0:
        print "Species group {0} is complete".format(group)

    elif len(missing_files) > 0:
        print "Missing {0} species in composite; they are: {1}".format(len(missing_files), missing_files)

        if len(extrafiles) > 0:
            print "Extra {0} species in composite; they are: {1}".format(len(extrafiles), extrafiles)

    elif len(extrafiles) > 0:
            print "Extra {0} species in composite; they are: {1}".format(len(extrafiles), extrafiles)

    for species in species_in_comp:  # Checks if a species has multiple rows in composite
        num_occurences = species_in_comp.count(species)
        if num_occurences > 1:
            print "Species {0} has multiple row of data".format(species)


end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
