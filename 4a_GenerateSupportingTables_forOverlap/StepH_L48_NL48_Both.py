import pandas as pd

master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Source of Call final BE-Range', 'WoE Summary Group']
in_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20170131_yes_no_2.csv'
out_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\Range_Region_20170131.csv'
in_range = pd.read_csv(in_csv)
in_range['EntityID'] = in_range['EntityID'].map(lambda x: x).astype(str)
species_df = pd.read_csv(master_list, dtype=object)
species_df = species_df[col_include_output]
out_df = pd.DataFrame(columns=['EntityID', 'L48/NL48', 'Multiple NL48 Regions', 'Part of Range on uninhabited island'])
counter = 0
row_count = len(in_range)
print row_count

while counter < row_count:
    entid = str(in_range.iloc[counter, 0])
    l48 = in_range.ix[counter, ['CONUS']].values.tolist()

    nl48 = in_range.ix[counter, ['AK', 'AS', 'CNMI', 'HI', 'GU', 'PR', 'VI']].values.tolist()
    no_use_terr = in_range.ix[
        counter, ['Howland', 'Baker', 'Jarvis', 'Johnston', 'Laysan', 'Mona', 'Necker', 'Nihoa', 'NorthwesternHI',
                  'Palmyra Kingman', 'Wake']].values.tolist()
    if 'Yes' in l48 and 'Yes' in nl48:
        region = 'Both'
    elif 'Yes' in l48 and 'Yes' not in nl48:
        region = 'CONUS'
    elif 'Yes' not in l48 and 'Yes' in nl48:
        region = 'NL48'
    elif 'Yes' not in l48 and 'Yes' not in nl48 and 'Yes' in no_use_terr:
        region = 'NoOverlap_territory'
    else:
        region = 'No_GIS'

    if 'Yes' in nl48 and nl48.count('Yes') > 1:
        lregion = 'Yes'
    else:
        lregion = 'No'

    if 'Yes' in no_use_terr:
        un_inhab = 'Yes'
    else:
        un_inhab = 'No'

    # print '{0} has output of {1}'.format(entid, region)

    out_df = out_df.append({'EntityID': entid, 'L48/NL48': region, 'Multiple NL48 Regions': lregion,
                            'Part of Range on uninhabited island': un_inhab}, ignore_index=True)
    counter += 1
out_df['EntityID'] = out_df['EntityID'].map(lambda x: x).astype(str)

merged_df = pd.merge(species_df, out_df, on='EntityID', how='left')
merged_df.to_csv(out_csv)

merged_df.to_csv(out_csv)
