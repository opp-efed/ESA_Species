import pandas as pd

in_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_SpeciesRegions_all_20161130.csv'
out_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\Range_Region_Mag_20161130.csv'
in_range = pd.read_csv(in_csv)
out_df = pd.DataFrame(columns=['EntityID', 'Region'])
counter = 0
row_count = len(in_range)
print row_count

while counter < row_count:
    entid = in_range.iloc[counter,1]
    l48 = in_range.ix[counter, ['L48']].values.tolist()

    nl48 = in_range.ix[counter, ['AK', 'AS', 'CNMI','HI', 'GU', 'PR', 'VI']].values.tolist()
    no_use_terr = in_range.ix[counter, ['Howland', 'Baker', 'Jarvis', 'Johnston', 'Laysan', 'Mona', 'Necker', 'Nihoa',
                                  'NorthwesternHI','Palmyra Kingman', 'Wake']].values.tolist()
    if 'Yes' in l48 and 'Yes' in nl48:
        region = 'Both'
    elif 'Yes' in l48 and 'Yes' not in nl48:
        region ='L48'
    elif 'Yes' not in l48 and 'Yes' in nl48:
        region = 'NL48'
    elif 'Yes' not in l48  and 'Yes' not in nl48 and 'Yes' in no_use_terr:
        region = 'NoOverlap_territory'
    else:
        region ='No_GIS'
    #print '{0} has output of {1}'.format(entid, region)
    out_df= out_df.append({'EntityID': entid, 'Region':region}, ignore_index=True)
    counter+=1
out_df.to_csv(out_csv)
