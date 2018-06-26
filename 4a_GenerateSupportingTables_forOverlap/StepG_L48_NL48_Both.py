import pandas as pd
import math

master_list = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSupportingTables\MasterLists\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group','Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename']

# set as Range of CH acres tables
in_csv = r'C:\Users\JConno02\Documents\Projects\ESA\CompositeFiles_Winter2018\CH_Acres_by_region_20180110.csv'
out_csv = r'C:\Users\JConno02\Documents\Projects\ESA\CompositeFiles_Winter2018\CH_Region_20180110.csv'
in_range = pd.read_csv(in_csv)
in_range['EntityID'] = in_range['EntityID'].map(lambda x: x).astype(str)
species_df = pd.read_csv(master_list, dtype=object)
species_df = species_df[col_include_output]
out_df = pd.DataFrame(columns=['EntityID', 'L48/NL48', 'Multiple NL48 Regions', 'Part of Range on uninhabited island'])
counter = 0
row_count = len(in_range)
print row_count

while counter < row_count:
    entid = str(in_range.iloc[counter, 1])  #NOTE Be sure entity is in pos 1 and not 0
    l48 = in_range.ix[counter, ['Acres_CONUS']].iloc[0]
    in_range['NL48_Sum'] = in_range[['Acres_AK','Acres_AS', 'Acres_CNMI', 'Acres_HI', 'Acres_GU', 'Acres_PR', 'Acres_VI']].sum(axis=1)
    nl_values = in_range.ix[counter,['Acres_AK','Acres_AS', 'Acres_CNMI', 'Acres_HI', 'Acres_GU', 'Acres_PR', 'Acres_VI']].values.tolist()
    nl_values = ['Yes' for v in nl_values if v >0]
    in_range['Island_Sum'] = in_range[['Acres_Howland Baker Jarvis',  'Acres_Johnston', 'Acres_Laysan',
                                       'Acres_Mona', 'Acres_Necker', 'Acres_Nihoa', 'Acres_NorthwesternHI',
                                       'Acres_Palmyra Kingman', 'Acres_Wake']].sum(axis=1)

    nl48 = in_range.ix[counter, ['NL48_Sum']].iloc[0]
    no_use_terr = in_range.ix[counter, ['Island_Sum']].iloc[0]

    if  l48 > 0 and  nl48 > 0:
        region = 'Both'
    elif l48 > 0 and math.isnan(nl48) == True:
        region = 'CONUS'
    elif math.isnan(l48) == True and nl48 > 0:
        region = 'NL48'
    elif math.isnan(l48) == True and math.isnan(nl48) == True and no_use_terr >0:
        region = 'NoOverlap_territory'
    else:
        region = 'No_GIS'

    if  nl48 >0 and nl_values.count('Yes') > 1:
        lregion = 'Yes'
    else:
        lregion = 'No'

    if  no_use_terr>0:
        un_inhab = 'Yes'
    else:
        un_inhab = 'No'

    out_df = out_df.append({'EntityID': entid, 'L48/NL48': region, 'Multiple NL48 Regions': lregion,
                            'Part of Range on uninhabited island': un_inhab}, ignore_index=True)
    counter += 1
out_df['EntityID'] = out_df['EntityID'].map(lambda x: x).astype(str)

merged_df = pd.merge(species_df, out_df, on='EntityID', how='left')
merged_df.to_csv(out_csv)

merged_df.to_csv(out_csv)
