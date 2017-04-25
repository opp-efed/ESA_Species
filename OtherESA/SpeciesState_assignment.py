import arcpy
import os
import pandas as pd

import datetime

# location of files/Workspace

infcs = [
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Amphibians_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Arachnids_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Birds_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Clams_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Corals_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Crustaceans_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Fishes_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Insects_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Mammals_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Reptiles_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Snails_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Lichens_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Ferns_and_Allies_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Flowering_Plants_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Conifers_and_Cycads_Composite_MAG_20161102'
]

raw_states = 'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Boundaries.gdb\State_territories_Regions'
# raw_states = 'L:\Workspace\ESA_Species\Range\Cnty_State_Range20150410\ESA_State_CntyRanges_Report2015Jan.gdb\Boundaries\States_tiger2014'
masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSVs\MasterListESA_June2016_20170117.csv'

state_filter = ['Massachusetts', 'Rhode Island']
outLocation = "L:\Workspace\ESA_Species\GMOs\SpeciesbyState"
outfilename_all = outLocation + os.sep + 'R_ESA_20170418_all_states.csv'
outfilename_states = outLocation + os.sep + 'R_ESA_20170418_filter_states.csv'

entid_index = 0

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

master_df = pd.read_csv(masterlist)
master_df['EntityID'] = master_df['EntityID'].map(lambda x: str(x))

# Empty df to store information about each of the feature classes
SpeTables = pd.DataFrame(columns=[u'EntityID', u'GEOID', u'STUSPS', u'NAME', u'Region'])

arcpy.MakeFeatureLayer_management(raw_states, "st_clp_lyr")
result = arcpy.GetCount_management("st_clp_lyr")
count_cnty = int(result.getOutput(0))

for fc in infcs:
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
    path, tail = os.path.split(fc)
    with arcpy.da.SearchCursor("fc_lyr", "EntityID") as cursor1:
        sp_cnt_total = arcpy.GetCount_management("fc_lyr")
        sp_counter = 1
        for sprow in cursor1:
            if sp_counter % 25 == 0 or sp_counter == 1:
                print 'Completed {0} of {1} in file {2}'.format(sp_counter, sp_cnt_total, tail)
            entid = str(sprow[entid_index])
            sp_counter += 1
            whereclause = "EntityID = '%s'" % entid
            arcpy.SelectLayerByAttribute_management("fc_lyr", "NEW_SELECTION", whereclause)
            arcpy.Delete_management("row_lyr")
            arcpy.MakeFeatureLayer_management("fc_lyr", "row_lyr")
            arcpy.SelectLayerByLocation_management("st_clp_lyr", "intersect", "row_lyr")
            arcpy.Delete_management("st_row_lyr")
            arcpy.MakeFeatureLayer_management("st_clp_lyr", "st_row_lyr")
            result = arcpy.GetCount_management("st_row_lyr")
            count_selection = int(result.getOutput(0))
            # print 'Species {0} is found in {2} of {1} raw_states'.format(entid, count_cnty, count_selection)
            if count_selection == 0:
                continue
            else:
                fieldList = [u'GEOID', u'STUSPS', u'NAME', u'Region']
                att_table = arcpy.da.TableToNumPyArray("st_row_lyr", fieldList)
                df = pd.DataFrame(data=att_table, columns=[u'EntityID', u'GEOID', u'STUSPS', u'NAME', u'Region'])
                df['EntityID'] = df[['EntityID']].fillna(str(entid))
                SpeTables = pd.concat([SpeTables, df], axis=0)

outDF = pd.merge(SpeTables, master_df, on='EntityID', how='outer')
outDF = outDF.reindex(columns=[u'EntityID', u'comname', u'sciname', u'status_text', u'Group', u'pop_desc', u'spcode',
                               u'vipcode', u'Range_Filename', u'WoE Summary Group', u'GEOID', u'STUSPS', u'NAME',
                               u'Region'])
# print outDF
outDF = outDF.drop_duplicates(subset=[u'EntityID', u'comname', u'sciname', u'status_text', u'Group', u'pop_desc',
                                      u'spcode', u'vipcode', u'Range_Filename', u'WoE Summary Group', u'GEOID',
                                      u'STUSPS', u'NAME', u'Region'], keep='first')

outDF.to_csv(outfilename_all)
if len(state_filter) != 0:
    stateDF = outDF.loc[outDF['NAME'].isin(state_filter)]
    stateDF.to_csv(outfilename_states)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
