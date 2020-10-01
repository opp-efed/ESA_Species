import os

path = r'D:\Tabulated_Habitat\Agg_Layers\Range'  # can be range or ch
# example folder with all species group to generate the list
list_csv = os.listdir(r'D:\Tabulated_Habitat\Agg_Layers\Range\CONUS_Atrazine_AA_Ag_June2020_euc')
# get list of species groups
list_spe = [v.split("_")[0] + v.split("_")[1] for v in list_csv]
list_spe = list(set(list_spe))
# list of output table types
suffix = ['adjHab.csv', 'noadjust.csv', 'HUC2AB.csv']
# list of folders, each folder represents a use
list_folder = os.listdir(path)
for folder in list_folder:
    print folder
    folder_path = path + os.sep + folder
    list_table = os.listdir(folder_path)
    csvs = [v for v in list_table if v.endswith('.csv')]
    for species in list_spe:

        species_region = False
        for csv in csvs:
            if csv.startswith(species):
                species_region = True
        if species_region:
            no_adjust = False
            huc = False
            habitat = False
            for csv in csvs:
                if csv.startswith(species) and csv.endswith(suffix[0]):
                    habitat = True
                if csv.startswith(species) and csv.endswith(suffix[1]):
                    no_adjust = True
                if csv.startswith(species) and csv.endswith(suffix[2]):
                    huc = True
            if not habitat or not no_adjust or not huc:
                print (
                    'Check species {0} in folder {1}, {2}, {3}, {4}'.format(species, folder, habitat, huc, no_adjust))
        else:
            print("species {0} is not in regions".format(species))
