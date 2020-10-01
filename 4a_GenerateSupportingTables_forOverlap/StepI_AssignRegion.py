import pandas as pd
import math
import datetime

# Author J.Connolly
# Internal deliberative, do not cite or distribute

master_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_June2020.csv"

# col to include in output
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_YesNo', 'Migratory', 'Migratory_YesNo',
                      'CH_Filename', 'Range_Filename']

# set as Range of CH acres tables
in_csv = r"D:\Species\Composites_Spring2020\RegionalFiles\Range\R_Acres_by_region_20200427.csv"
out_csv = r'D:\Species\Composites_Spring2020\RegionalFiles\Range\R_Regions_20200427.csv'  # path and file name

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

in_range = pd.read_csv(in_csv)
in_range['EntityID'] = in_range['EntityID'].map(lambda x: x).astype(str)
species_df = pd.read_csv(master_list, dtype=object)
species_df = species_df[col_include_output]
out_df = pd.DataFrame(columns=['EntityID', 'Regions', 'L48/NL48', 'Multiple NL48 Regions',
                               'Part of Range on uninhabited island'])
counter = 0
row_count = len(in_range)
print row_count

while counter < row_count:
    entid = str(in_range.iloc[counter, 1])  #NOTE Be sure entity is in pos 1 and not 0
    l48 = in_range.ix[counter, ['Acres_CONUS']].iloc[0]
    ak = in_range.ix[counter, ['Acres_AK']].iloc[0]
    as_r = in_range.ix[counter, ['Acres_AS']].iloc[0]
    cnmi= in_range.ix[counter, ['Acres_CNMI']].iloc[0]
    hi = in_range.ix[counter, ['Acres_HI']].iloc[0]
    gu = in_range.ix[counter, ['Acres_GU']].iloc[0]
    pr = in_range.ix[counter, ['Acres_PR']].iloc[0]
    vi = in_range.ix[counter, ['Acres_VI']].iloc[0]
    in_range['NL48_Sum'] = in_range[['Acres_AK','Acres_AS', 'Acres_CNMI', 'Acres_HI', 'Acres_GU', 'Acres_PR', 'Acres_VI']].sum(axis=1)
    nl_values = in_range.ix[counter,['Acres_AK','Acres_AS', 'Acres_CNMI', 'Acres_HI', 'Acres_GU', 'Acres_PR', 'Acres_VI']].values.tolist()
    nl_values = ['Yes' for v in nl_values if v >0]
    in_range['Island_Sum'] = in_range[['Acres_Howland Baker Jarvis',  'Acres_Johnston', 'Acres_Laysan',
                                       'Acres_Mona', 'Acres_Necker', 'Acres_Nihoa', 'Acres_NorthwesternHI',
                                       'Acres_Palmyra Kingman', 'Acres_Wake']].sum(axis=1)

    nl48 = in_range.ix[counter, ['NL48_Sum']].iloc[0]
    no_use_terr = in_range.ix[counter, ['Island_Sum']].iloc[0]
    region = ''
    if l48 > 0 and math.isnan(nl48) == True and math.isnan(no_use_terr) == True:
        region = 'CONUS'
        region_general = 'CONUS'
    elif l48 > 0 and math.isnan(nl48) == True and math.isnan(no_use_terr) == False:
        region = 'CONUS'
        region_general = 'CONUS and Uninhabited Islands'

    elif  l48 > 0 and  nl48 > 0:
        region_general = 'Both'
        region = 'CONUS, '

        if ak > 0:
            region = region + 'AK, '
        if as_r > 0:
            region = region + 'AS, '
        if cnmi > 0:
            region = region + 'CNMI, '
        if hi > 0:
            region = region + 'HI, '
        if gu > 0:
            region = region + 'GU, '
        if pr > 0:
            region = region + 'PR, '
        if vi > 0:
            region = region + 'VI, '
        if not math.isnan(no_use_terr):
            region = region + 'Uninhabited islands, '

    elif math.isnan(l48) == True and nl48 > 0:
        region_general = 'NL48'
        region = ''
        if ak > 0:
            region = region + 'AK, '
        if as_r > 0:
            region = region + 'AS, '
        if cnmi > 0:
            region = region + 'CNMI, '
        if hi > 0:
            region = region + 'HI, '
        if gu > 0:
            region = region + 'GU, '
        if pr > 0:
            region = region + 'PR, '
        if vi > 0:
            region = region + 'VI, '
        if not math.isnan(no_use_terr):
            region = region + 'Uninhabited islands, '
    elif math.isnan(l48) == True and math.isnan(nl48) == True and no_use_terr >0:
        region_general = 'Uninhabited islands'
    else:
        region = 'No_GIS'
        region_general = 'No_GIS'

    if  nl48 >0 and nl_values.count('Yes') > 1:
        lregion = 'Yes'
    else:
        lregion = 'No'

    if  no_use_terr>0:
        un_inhab = 'Yes'
    else:
        un_inhab = 'No'

    out_df = out_df.append({'EntityID': entid, 'Regions': region, 'L48/NL48': region_general,
                            'Multiple NL48 Regions': lregion, 'Part of Range on uninhabited island': un_inhab},
                           ignore_index=True)
    counter += 1
out_df['EntityID'] = out_df['EntityID'].map(lambda x: x).astype(str)

merged_df = pd.merge(species_df, out_df, on='EntityID', how='left')
merged_df.to_csv(out_csv)

merged_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
