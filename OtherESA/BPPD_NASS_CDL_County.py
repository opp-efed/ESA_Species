# import system modules
import arcpy
import os
import pandas as pd

import datetime
#TODO update so CH_Filenames is also included in final table'
# location of files/Workspace
entIndex = 0
# infcs = [
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Amphibians_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Arachnids_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Birds_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Clams_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Corals_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Crustaceans_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Fishes_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Insects_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Mammals_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Reptiles_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Snails_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Ferns_and_Allies_Composite_MAG_20161102',
#     'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Flowering_Plants_Composite_MAG_20161102',]

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
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Ferns_and_Allies_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Flowering_Plants_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Conifers_and_Cycads_Composite_MAG_20161102',
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Lichens_Composite_MAG_20161102',
]

counties = "L:\Workspace\ESA_Species\Range\Cnty_State_Range20150410\ESA_State_CntyRanges_Report2015Jan.gdb\Boundaries\Counties_NASS_Table170502"
masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSVs\MasterListESA_June2016_20170117.csv'

# TODO create county file with crop information on the fly at the beginning and point to raw county file in the path above
state_filter = []
outLocation = "C:\Users\JConno02\Documents\Projects\SmallProject\BPPD_overlap"
outfilename_all = outLocation + os.sep + 'R_ESA_Species_all_counties.csv'
outfilename_counties = outLocation + os.sep + 'R_ESA_Species_all_20170517_counties.csv'
outfilename_states = outLocation + os.sep + 'R_ESA_Species_all20170517_states.csv'

speinfo = ['EntityID', 'comname', 'sciname', 'spcode', 'vipcode', 'status_text', 'Group', 'Range_Filename']
useLookup = {'10': 'Corn',
             '20': 'Cotton',
             '30': 'Rice',
             '40': 'Soybean',
             '50': 'Wheat',
             '60': 'Veg Ground Fruit',
             '70': 'Other trees',
             '80': 'Other grains',
             '90': ' Other row crops',
             '100': 'Other Crops',
             '110': 'Pasture/Hay/Forage'
             }

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

arcpy.MakeFeatureLayer_management(counties, "cntyclp_lyr")
result = arcpy.GetCount_management("cntyclp_lyr")
count_cnty = int(result.getOutput(0))

fieldList = [f.name for f in arcpy.ListFields(counties) if not f.required]
df_fields = fieldList.append('EntityID')
Spe_df = pd.DataFrame(columns=df_fields)  # Empty df to store information about each of the feature classes

for fc in infcs:
    print fc
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
    path, tail = os.path.split(fc)
    group = tail.split("_")
    group = str(group[1])
    with arcpy.da.SearchCursor("fc_lyr", ['EntityID']) as cursor1:
        for sprow in cursor1:
            colindexspec = 0
            entid = str(sprow[entIndex])

            whereclause = "EntityID = '%s'" % entid
            arcpy.SelectLayerByAttribute_management("fc_lyr", "NEW_SELECTION", whereclause)
            arcpy.Delete_management("row_lyr")
            arcpy.MakeFeatureLayer_management("fc_lyr", "row_lyr")
            arcpy.SelectLayerByLocation_management("cntyclp_lyr", "intersect", "row_lyr")

            arcpy.Delete_management("cntyrow_lyr")
            arcpy.MakeFeatureLayer_management("cntyclp_lyr", "cntyrow_lyr")
            result = arcpy.GetCount_management("cntyrow_lyr")
            count_selection = int(result.getOutput(0))
            print 'Species {0} is found in {2} of {1} counties'.format(entid, count_cnty, count_selection)
            # for NL48 species no county will be selected therefore feature layer will equal all counties, and not selection, these species should be skipped
            if count_selection == 0:
                continue
            else:
                try:
                    fieldList2 = [f.name for f in arcpy.ListFields("cntyrow_lyr") if not f.required]
                    arr = arcpy.da.TableToNumPyArray("cntyrow_lyr", fieldList2)
                    ent_df = pd.DataFrame(arr, columns=df_fields)
                    ent_df.loc[:, 'EntityID'] = entid
                    Spe_df = pd.concat([Spe_df, ent_df], axis=0)
                except TypeError:
                    print 'Failed for species {0}'.format(entid)

df_master = pd.read_csv(masterlist)
df_master['EntityID'] = df_master['EntityID'].astype(str)
df_master = df_master.reindex(columns=speinfo)

out_df = pd.merge(Spe_df, df_master, on='EntityID', how='left')

out_columns = out_df.columns.values.tolist()
final_cols = []

for i in out_columns:
    check = i.split("_")
    if check[0] == 'CDL':
        code = str(check[1])
        use = useLookup[code]
        i = 'CDL_' + use
    elif check[0] == 'NASS':
        code = str(check[1])
        use = useLookup[code]
        i = 'NASS_' + use
    else:
        i = i
    final_cols.append(str(i))

print final_cols

out_df.columns = final_cols
out_df= out_df.reindex(columns=
               ['EntityID', 'comname', 'sciname', 'spcode', 'vipcode', 'status_text', 'Group', 'Range_Filename',
                'County_Name', 'State', 'ST_ABB', 'STATEFP', 'GEOID', 'NASS_Corn', 'NASS_Cotton', 'NASS_Rice',
                'NASS_Soybean', 'NASS_Wheat', 'NASS_Veg Ground Fruit', 'NASS_Other trees', 'NASS_Other grains',
                'NASS_ Other row crops', 'NASS_Other Crops', 'NASS_Pasture/Hay/Forage', 'CDL_Corn', 'CDL_Cotton',
                'CDL_Rice', 'CDL_Soybean', 'CDL_Wheat', 'CDL_Veg Ground Fruit', 'CDL_Other trees', 'CDL_Other grains',
                'CDL_ Other row crops', 'CDL_Other Crops', 'CDL_Pasture/Hay/Forage'])
out_df.to_csv(outfilename_all, encoding='utf-8')

stateDF = out_df.reindex(
    columns=['EntityID', 'comname', 'sciname', 'spcode', 'vipcode', 'status_text', 'Group', 'Range_Filename', 'State'])
stateDF = stateDF.drop_duplicates(
    subset=['EntityID', 'comname', 'sciname', 'spcode', 'vipcode', 'status_text', 'Group', 'Range_Filename', 'State'],
    keep='first')

print stateDF
if len(state_filter) != 0:
    out_df = out_df.loc[out_df['State'].isin(state_filter)]
    stateDF = stateDF.loc[stateDF['State'].isin(state_filter)]

out_df.to_csv(outfilename_counties, encoding='utf-8')
stateDF.to_csv(outfilename_states, encoding='utf-8')

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
