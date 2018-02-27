import pandas as pd


master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410.csv'
overlaping_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\combine.csv'

species_df = pd.read_csv(master_list)
overlapping_df = pd.read_csv(overlaping_table)

list_sp = species_df['EntityID'].values.tolist()
list_overlap = overlapping_df['Species'].values.tolist()


sp_arry = pd.DataFrame(columns=list_sp)
sp_arry['EntityID'] = list_sp
list_sp.insert(0,'EntityID')
sp_arry = sp_arry.reindex(columns=list_sp)
sp_arry.fillna(0,inplace=True)
list_sp.remove('EntityID')
for species in list_sp:
    for v in list_overlap:
        overlapping_group = v.split(",")
        if species in overlapping_group:
            overlapping_group.remove(species)
            count = overlapping_df.loc[overlapping_df['Species'] ==v, 'Count'].iloc[0]
            for i in overlapping_group:
                sp_over = str(i).replace(" ","")
                if sp_over in list_sp:
                    current_value = sp_arry.loc[sp_arry['EntityID'] == species, sp_over].iloc[0]
                    add_value = int(current_value) + int(count)
                    sp_arry.loc[sp_arry['EntityID'] == species, sp_over] = add_value
                else:
                    pass

        else:
            pass

sp_arry.to_csv(r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\out.csv')