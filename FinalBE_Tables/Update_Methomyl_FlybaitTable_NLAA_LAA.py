import pandas as pd
import datetime
import os

# new_list_df =new_list_df.loc[new_list_df['Fly bait Only'].isin(zones) == True]

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

plant_groups = ['Flowering Plants', 'Conifers and Cycads', 'Ferns and Allies', 'Lichens', 'Corals']

in_ED_plants = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait\NLAA_LAA_Calls_plants.csv'
in_dropped_plants = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait\TerrPlant_Dropped_20170207.csv'
current_plants = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait\TerrPlant_Updated_20170207.csv'

in_ED_animals = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait\NLAA_LAA_Calls_animals.csv'
in_dropped_animals = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait\TerrAnimal_Dropped_20170207.csv'
current_animals = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait\TerrAnimal_Updated_20170207.csv'

outlocation ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\DraftBEs\Methomyl\Flybait'
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

plants_ed_df = pd.read_csv(in_ED_plants)
plants_ed_df['FWS/NMFS Species ID'] = plants_ed_df['FWS/NMFS Species ID'].map(lambda x: x).astype(str)
plants_dropped_df = pd.read_csv(in_dropped_plants)
plants_dropped_df['EntityID'] = plants_dropped_df['EntityID'].map(lambda x: x).astype(str)
plants_current_df = pd.read_csv(current_plants)
plants_current_df['EntityID'] = plants_current_df['EntityID'].map(lambda x: x).astype(str)

dropped_plants = plants_dropped_df['EntityID'].values.tolist()
plants_ed_df = plants_ed_df.loc[plants_ed_df['FWS/NMFS Species ID'].isin(dropped_plants) == False]
tracking_entid_plants = plants_ed_df['FWS/NMFS Species ID'].values.tolist()
added_plants = plants_current_df.loc[plants_current_df['EntityID'].isin(tracking_entid_plants) == False]
append_plants = added_plants.loc[:, ['Scientific Name', 'Common Name', 'EntityID']]
append_plants.columns = ['Scientific Name', 'Common Name', 'FWS/NMFS Species ID']
append_plants= append_plants.reindex(
    columns=['Scientific Name', 'Common Name', 'Listing Status1', 'FWS/NMFS Species ID', 'Risk (Direct Effects)',
             'Confidence (Direct Effects)', 'Risk (Indirect Effects)',
             'Confidence (Indirect Effects)', 'Species Call2', 'Critical Habitat Call2'])

plants_ed_df = pd.concat([plants_ed_df, append_plants], axis=0)

animals_ed_df = pd.read_csv(in_ED_animals)
animals_ed_df['FWS/NMFS Species ID'] = animals_ed_df['FWS/NMFS Species ID'].map(lambda x: x).astype(str)
animals_dropped_df = pd.read_csv(in_dropped_animals)
animals_dropped_df['EntityID'] = animals_dropped_df['EntityID'].map(lambda x: x).astype(str)
animals_current_df = pd.read_csv(current_animals)
animals_current_df['EntityID'] = animals_current_df['EntityID'].map(lambda x: x).astype(str)

dropped_animals = animals_dropped_df['EntityID'].values.tolist()
animals_ed_df = animals_ed_df.loc[animals_ed_df['FWS/NMFS Species ID'].isin(dropped_animals) == False]
tracking_entid_animals = animals_ed_df['FWS/NMFS Species ID'].values.tolist()
added_animals = animals_current_df.loc[animals_current_df['EntityID'].isin(tracking_entid_animals) == False]
append_animals = added_animals.loc[:, ['Scientific Name', 'Common Name', 'EntityID']]
append_animals.columns = ['Scientific Name', 'Common Name', 'FWS/NMFS Species ID']
append_animals= append_animals.reindex(
    columns=['Scientific Name', 'Common Name', 'Listing Status1', 'FWS/NMFS Species ID', 'Risk (Direct Effects)',
             'Confidence (Direct Effects)', 'Risk (Indirect Effects)',
             'Confidence (Indirect Effects)', 'Species Call2', 'Critical Habitat Call2'])

animals_ed_df = pd.concat([animals_ed_df, append_animals], axis=0)




#
animals_ed_df.to_csv(outlocation + os.sep + 'TerrAnimal_EDTable_' + date + '.csv')
plants_ed_df.to_csv(outlocation + os.sep + 'TerrPlant_EDTable_' + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
