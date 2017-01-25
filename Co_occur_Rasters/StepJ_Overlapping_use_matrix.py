import pandas as pd
import os
import datetime

infolder = r'L:\Workspace\UseSites\ByProject\Overlapping_Use'
out_csv = infolder+os.sep+'Overlapping_Use_Matrix_b.csv'
list_folder= os.listdir(infolder)
list_folder.remove('Overlapping_Use_Matrix.csv')
list_folder.remove('Overlapping_Use_Matrix_b.csv')
useLookup = {'10': 'Corn',
             '20': 'Cotton',
             '30': 'Rice',
             '40': 'Soybeans',
             '50': 'Wheat',
             '60': 'Vegetables and Ground Fruit',
             '70': 'Orchards and Vineyards',
             '80': 'Other Grains',
             '90': 'Other RowCrops',
             '100': 'Other Crops',
             '110': 'Pasture',
             'CattleEarTag': 'Cattle Eartag',
             'Developed': 'Developed',
             'ManagedForests': 'Managed Forest',
             'Nurseries': 'Nurseries',
             'OSD': 'Open Space Developed',
             'ROW': 'Right of Way',
             'CullPiles': 'Cull Piles',
             'Cultivated': 'Cultivated',
             'NonCultivated': 'Non Cultivated',
             'PineSeedOrchards': 'Pineseed Orchards',
             'XmasTrees': 'Christmas Tree',
             'Diazinon': 'Diazinon_AA',
             'Carbaryl': 'Carbaryl_AA',
             'Chlorpyrifos': 'Chlorpyrifos_AA',
             'Methomyl': 'Methomyl_AA',
             'Malathion': 'Malathion_AA',
             'usa': 'Golf Courses',
             'bermudagrass2': 'Bermuda Grass'}

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_use_raw = useLookup.keys()
final_uses = sorted(useLookup.values())
out_matrix = pd.DataFrame(index=final_uses,columns=final_uses)

for folder in list_folder:
    current_path = infolder+os.sep+folder
    list_csv = os.listdir(current_path)
    list_csv =[csv for csv in list_csv if csv.endswith('csv')]
    for csv in list_csv:
        current_uses =[]
        parse_name = csv.split("_")
        for use_value in parse_name:
            if use_value in list_use_raw:
                current_uses.append(use_value)
            else:
                parse_name
        in_df = pd.read_csv(current_path+os.sep+csv)
        pixel_count = int(in_df.ix[0,'Value_0'])
        use_1 = current_uses[0]
        use_2 = current_uses[1]
        out_use_1 = useLookup[use_1]
        out_use_2 = useLookup[use_2]
        out_matrix.loc[out_use_1,out_use_2]= pixel_count
        out_matrix.loc[out_use_2,out_use_1]=pixel_count

print out_matrix
msq_overlap = out_matrix.multiply(900)
acres_overlap = msq_overlap.multiply(0.000247)
#out_df = acres_overlap.round(0)
acres_overlap.to_csv(out_csv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
