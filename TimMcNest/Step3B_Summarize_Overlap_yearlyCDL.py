import os
import pandas as pd
import datetime

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

infolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Results_Fall2016\Indiv_Year_Drift\CONUS'
##export att table of refuge clips of hab
outlocation = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Tables_Fall2016\Ind_year_drift\SumSpecies'
completedlist = []
list_all_csv = os.listdir(infolder)
list_all_csv = [csv for csv in list_all_csv if csv.endswith('csv')]
bins = [0, 305, 765]

speHabitDict = {'94': [286, 38, 97, 90],
                '123': [39, 41, 42, 45, 56, 148, 179, 277, 278, 281, 282, 296, 297, 298, 300, 301, 302, 303, 304, 305,
                        358, 359, 360, 556, 557, 558, 559, 562, 563, 581, 582, 583],
                '133': [290, 291, 292, 293, 294, 295, 363, 370, 379, 407, 408, 410, 411, 449, 556, 557, 574, 575],
                '145': [296, 297, 298, 300, 302, 303, 359, 360, 383, 384, 385, 470, 471, 472, 476, 485, 489],
                '6901': [39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 54, 55, 56, 57, 58, 136, 137, 138, 139, 140, 141,
                         142, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 158, 159, 160, 161, 162,
                         163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 181, 182,
                         183, 184, 185, 186, 187, 188, 189, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276,
                         277, 278, 279, 280, 281, 282, 356, 357, 358, 359, 360, 361, 424, 425, 427, 555],

                '10147': [333, 335, 337, 422, 424, 426],
                '89': [539, 541, 553, 460, 461, 462, 466, 467, 468, 469, 472, 473, 474, 475, 476, 477, 358, 443, 444],
                '139': [30, 46, 50, 51, 52, 53, 59, 65, 70, 91, 104, 188, 193, 197, 198, 200, 222, 223, 235, 240, 253,
                        281, 283, 284],
                '149': [39, 40, 41, 42, 43, 45, 46, 47, 48, 49, 55, 56, 145, 146, 148, 49, 151, 152, 153, 154, 155, 156,
                        158, 159, 162, 163, 164, 165, 175, 179, 181, 183, 184, 185, 186, 187, 188, 189, 194, 266, 270,
                        271, 272, 277, 278, 280, 281, 282, 296, 297, 298, 300, 301, 302, 303, 304, 305, 309, 315, 316,
                        317, 323, 326, 329, 330, 331, 356, 357, 358, 359, 360, 361, 383, 384, 385, 432, 433, 438, 439,
                        442, 443, 444, 445, 455, 457, 458, 459, 509, 578, 579],
                '83': [556, 557, 20, 30, 70, 222, 223, 233, 235, 284, 560, 562, 479, 480, 481, 482, 483, 339, 340, 372,
                       378, 392, 393, 415, 452, 454],

                '138': [30, 46, 47, 50, 51, 52, 53, 59, 65, 70, 91, 104, 188, 189, 192, 193, 197, 198, 200, 222, 223,
                        235, 240, 253, 271, 281, 283, 284, 317, 324, 328, 329, 330, 331, 338, 339, 358, 392, 426, 442,
                        459, 460, 461, 462, 463, 464, 465, 467, 468, 469, 475, 479, 481, 482, 483, 556, 557, 558, 559,
                        560],
                '137': [444, 457, 459, 470, 471, 472, 476, 477, 489, 581],
                '116': [303, 304, 385, 432],
                '4064': [315, 316, 317, 323, 326, 438, 439, 502, 503, 556, 558, 559],

                }

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

def summarize_species(list_all_csv, multi, multi_break, use_index):
    out_species_df = (pd.DataFrame(columns=['Use', 'CDL Year', 'Spray Drift', 'Count_Pref', 'Count_Full']))
    csv_list_species = [csv for csv in list_all_csv if str(csv.split("_")[5]) == entid]
    if multi:
        csv_list_species = [csv.replace("__", "_") for csv in csv_list_species]
        if multi_break == '':
            csv_list_species = [csv for csv in csv_list_species if str(csv.split("_")[6]) == 'CDL']
        else:
            csv_list_species = [csv for csv in csv_list_species if str(csv.split("_")[6]) == multi_break]
    for csv in csv_list_species:
        out_use_df = pd.DataFrame()
        use_value = csv.split("_")[use_lookup_index]
        cdl_year = csv.split("_")[use_lookup_index - 1]
        use = useLookup[use_value]
        in_use_df = pd.read_csv((infolder + os.sep + csv), dtype=object)

        in_use_df['LABEL'] = in_use_df['LABEL'].astype(str)
        in_use_df['LABEL'] = in_use_df['LABEL'].map(lambda x: x.replace(',', '')).astype(long)
        interval_values = in_use_df['LABEL'].values.tolist()
        columns = in_use_df.columns.values.tolist()
        full_range_columns = [col for col in columns if col.startswith('Value')]
        full_range_df = in_use_df.ix[:, full_range_columns].apply(pd.to_numeric)
        full_range_df['sum_full'] = full_range_df.sum(axis=1)

        preferred_hab = speHabitDict[entid]
        cols_preferred = []
        for col in columns:
            try:
                if int(col.split('_')[1]) in preferred_hab:
                    cols_preferred.append(col)
                else:
                    pass
            except:
                pass

        preferred_df = in_use_df.ix[:, cols_preferred].apply(pd.to_numeric)
        preferred_df["sum"] = preferred_df.sum(axis=1)
        out_use_df['preferred_sum'] = preferred_df.ix[:, 'sum']
        out_use_df['full_sum'] = full_range_df['sum_full']
        transformed_out = out_use_df.T

        transformed_out.columns = interval_values
        for value in bins:

            binned_col = []
            for col in interval_values:
                get_interval = int(col)
                if get_interval <= value:
                    binned_col.append(col)
            binned_df = transformed_out.ix[:, binned_col]

            sum_bin = binned_df.sum(axis=1)
            pref_sum = int(sum_bin.get('preferred_sum'))
            full_sum = int(sum_bin.get('full_sum'))
            out_species_df = out_species_df.append(
                {'Use': str(use), 'CDL Year': int(cdl_year), 'Spray Drift': str(value),
                 'Count_Pref': int(pref_sum), 'Count_Full': int(full_sum)},
                ignore_index=True)
    return out_species_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

entList = speHabitDict.keys()

for entid in entList:
    if entid == '94':
        break_species = True
        for species_range_break in ['migration', 'breeding', '']:
            if species_range_break == '':
                use_lookup_index = 8
            else:
                use_lookup_index = 9
            out_df = summarize_species(list_all_csv, break_species, species_range_break, use_lookup_index)
            outcsv = outlocation + os.sep + 'SummaryTable_Count_' + str(entid) + '_' + species_range_break + "_" + str(
                date) + '.csv'
            outcsv = outcsv.replace("__", "_")
            out_df.to_csv(outcsv)
            print 'Exported table for species {0}, {1}'.format(entid, species_range_break)
    else:
        break_species = False
        species_range_break = 'nan'
        use_lookup_index = 8
        out_df = summarize_species(list_all_csv, break_species, species_range_break, use_lookup_index)
        outcsv = outlocation + os.sep + 'SummaryTable_Count_' + str(entid) + '_' + str(date) + '.csv'
        out_df.to_csv(outcsv)
        print 'Exported table for species {0}'.format(entid)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
