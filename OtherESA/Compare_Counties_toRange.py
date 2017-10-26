import pandas as pd
import datetime

master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410.csv'
county_list = r'C:\Users\JConno02\Documents\Projects\ESA\ReportsFromKeith\20160513_SpeciesByCounty_onMaster_HasFIPS.csv'
range_list = r'L:\Workspace\ESA_Species\GMOs\SpeciesbyState\ESA_Species_all_20170510_counties.csv'

species_df = pd.read_csv(master_list, dtype=object)
county_df = pd.read_csv(county_list, dtype=object)
range_df = pd.read_csv(range_list, dtype=object)
list_sp = species_df['EntityID'].values.tolist()

sp_array = pd.DataFrame(data=list_sp, columns=['EntityID'])
sp_array = sp_array.reindex(columns=['EntityID', 'Extra County Report', 'Extra Range'])

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

for species in list_sp:
    if list_sp.index(species) % 25 == 0 or list_sp.index(species) == 0:
        print 'Working on {0} of {1}...'.format(list_sp.index(species), len(list_sp))
    county_selection = county_df.loc[county_df['EntityID'] == species]
    species_selection = range_df.loc[range_df['EntityID'] == species]

    check_extra_county = county_selection.loc[
        ~county_selection['FIPS'].isin(species_selection['GEOID'].values.tolist())]
    check_extra_species = species_selection.loc[
        ~species_selection['GEOID'].isin(county_selection['FIPS'].values.tolist())]
    if len(check_extra_county) > 0:
        sp_array.loc[sp_array['EntityID'] == species, 'Extra County Report'] = 'x'
    elif len(check_extra_species) > 0:
        sp_array.loc[sp_array['EntityID'] == species, 'Extra Range'] = 'x'
    else:
        pass
merged_df = pd.merge(species_df, sp_array, on='EntityID', how='outer')
merged_df.to_csv(r'C:\Users\JConno02\Documents\Projects\ESA\ReportsFromKeith\CompareRange_CountyReport.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: {0}".format(end - start_time)
