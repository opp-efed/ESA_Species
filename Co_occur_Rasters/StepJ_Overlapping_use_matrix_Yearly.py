import pandas as pd
import os
import datetime

infolder = r'L:\Workspace\UseSites\ByProject\Overlapping_Yearly'
out_location = r'L:\Workspace\UseSites\ByProject\Overlapping_Yearly_Summary'
out_csv_count = out_location + os.sep + 'Overlapping_Use_Matrix_count'
out_csv_acres = out_location + os.sep + 'Overlapping_Use_Matrix_acres'

list_folder = os.listdir(infolder)

cdl_recode = {'Unnamed: 0': 'Unnamed: 0',
              'OBJECTID': 'OBJECTID',
              'LABEL': 'LABEL',
              '0': 'BackGround',
              '10': 'Corn',
              '14': 'Corn/soybeans',
              '15': 'Corn/wheat',
              '18': 'Corn/grains',
              '20': 'Cotton',
              '25': 'Cotton/wheat',
              '26': 'Cotton/vegetables',
              '30': 'Rice',
              '40': 'Soybeans',
              '42': 'Soybeans/cotton',
              '45': 'Soybeans/wheat',
              '48': 'Soybeans/grains',
              '50': 'Wheat',
              '56': 'Wheat/vegetables',
              '58': 'Wheat/grains',
              '60': 'Vegetables and ground fruit',
              '61': '(ground fruit)',
              '68': 'Vegetables/grains',
              '70': 'Orchards and grapes',
              '75': 'Other trees',
              '80': 'Other grains',
              '90': 'Other row crops',
              '100': 'Other crops',
              '110': 'Pasture/hay/forage',
              '121': 'Developed - open',
              '122': 'Developed - low',
              '123': 'Developed - med',
              '124': 'Developed - high',
              '140': 'Forest',
              '160': 'Shrubland',
              '180': 'Water',
              '190': 'Wetlands - woods',
              '195': 'Wetlands - herbaceous',
              '200': 'Miscellaneous land',

              }

collaspes_ag = {
    'Corn': ['Corn', 'Corn/soybeans', 'Corn/wheat', 'Corn/grains'],
    'Cotton': ['Cotton', 'Cotton/wheat', 'Cotton/vegetables'],
    'Orchards and Vineyards': ['Orchards and grapes'],
    'Other Crops': ['Other crops'],
    'Other Grains': ['Other grains'],
    'Other RowCrops': ['Other row crops'],
    'Pasture': ['Pasture/hay/forage'],
    'Rice': ['Rice'],
    'Soybeans': ['Soybeans', 'Soybeans/cotton', 'Soybeans/wheat', 'Soybeans/grains'],
    'Vegetables and Ground Fruit': ['Vegetables and ground fruit', '(ground fruit)', 'Vegetables/grains'],
    'Wheat': ['Wheat', 'Wheat/vegetables', 'Wheat/grains'],

}
useLookup = {
    'CattleEarTag': 'Cattle Eartag',
    'Developed': 'Developed',
    'ManagedForests': 'Managed Forest',
    'Nurseries': 'Nurseries',
    'OSD': 'Open Space Developed',
    'ROW': 'Right of Way',
    'CullPiles': 'Cull Piles',
    'PineSeedOrchards': 'Pineseed Orchards',
    'XmasTrees': 'Christmas Tree',
    'Diazinon': 'Diazinon_AA',
    'Methomyl':'Methomyl_AA',

    'Chlorpyrifos': 'Chlorpyrifos_AA',

    'Malathion': 'Malathion_AA',
    'usa': 'Golf Courses',

}
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_use_raw_non_ag = useLookup.keys()
final_uses = sorted(useLookup.values())
list_use_raw_ag = collaspes_ag.keys()
col_header = list_use_raw_ag

for folder in list_folder:
    out_matrix = pd.DataFrame(index=final_uses, columns=sorted(col_header))
    list_csv = os.listdir(infolder + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    for csv in list_csv:

        print csv
        current_uses = []
        parse_name = csv.split("_")
        non_ag = str(parse_name[8])
        in_df = pd.read_csv(infolder + os.sep + folder + os.sep + csv)
        in_df.drop('TableID', axis=1, inplace=True)
        try:
            in_df.drop('Value_73', axis=1, inplace=True)
        except:
            pass
        out_cols = []
        current_cols = in_df.columns.values.tolist()
        for i in current_cols:
            if i.startswith('Value'):
                lookup_values = i.split("_")[1]
            else:
                lookup_values = i.split("_")[0]
            i_out = cdl_recode[lookup_values]
            out_cols.append(i_out)
        in_df.columns = out_cols
        pixel_count = (in_df.ix[0, :])
        for ag_use in list_use_raw_ag:
            list_uses = collaspes_ag[ag_use]
            use_df = pixel_count[list_uses]
            sum_df = use_df.sum()
            final_nonag = useLookup[non_ag]
            out_matrix.ix[final_nonag ,ag_use]= sum_df

    out_csv_count_a = out_csv_count+ "_" + folder+'.csv'
    out_matrix.to_csv(out_csv_count_a)
    msq_overlap = out_matrix.multiply(900)
    acres_overlap = msq_overlap.multiply(0.000247)
    #out_df = acres_overlap.round(0)
    out_csv_acres_a =out_csv_acres+"_" + folder+'.csv'
    acres_overlap.to_csv(out_csv_acres_a)


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
