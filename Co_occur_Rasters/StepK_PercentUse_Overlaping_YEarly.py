import pandas as pd
import os
import datetime
import arcpy

in_table = r'L:\Workspace\UseSites\ByProject\Overlapping_Yearly_Summary'
in_gdb = r'L:\Workspace\UseSites\CDL_Reclass\161031\CDL_Reclass_1015_161031.gdb'
out_csv = r'L:\Workspace\UseSites\ByProject\Overlapping_Yearly_Summary'

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
    'Corn': [10, 14, 15, 18],
    'Cotton': [20, 25, 26],
    'Orchards and Vineyards': [70],
    'Other Crops': [100],
    'Other Grains': [80],
    'Other RowCrops': [90],
    'Pasture': [110],
    'Rice': [30],
    'Soybeans': [40, 42, 45, 48],
    'Vegetables and Ground Fruit': [60, 61, 68],
    'Wheat': [50, 58, 56],

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

arcpy.env.workspace = in_gdb
use_raster = arcpy.ListRasters()

list_use_raw_non_ag = useLookup.keys()
final_uses = sorted(useLookup.values())
list_use_raw_ag = collaspes_ag.keys()
col_header = list_use_raw_ag

list_csv_in = os.listdir(in_table)
list_csv_acres = [csv for csv in list_csv_in if csv.split("_")[3]=='acres']
print list_csv_acres

for csv in list_csv_acres:
    in_acres_df = pd.read_csv(in_table +os.sep +csv)
    year = csv.split("_")[5]
    out_matrix_percent = pd.DataFrame(index=final_uses, columns=sorted(col_header))

    for raster in use_raster:
        parse_name = raster.split("_")
        if year in parse_name:
            in_raster = in_gdb + os.sep + raster
            att_array = arcpy.da.TableToNumPyArray((in_raster), ['Value','Count'])
            att_df = pd.DataFrame(data=att_array)
            print att_df
            for use in list_use_raw_ag:
                list_values_use =collaspes_ag[use]
                use_df = att_df[att_df['Value'].isin(list_values_use) ==True]
                sum_df= use_df.sum()
                pixel_count_use = int(sum_df.get('Count'))
                print pixel_count_use
                msq_use = int(pixel_count_use)*(900)
                acres_use = int(msq_use)*(0.000247)
                print acres_use
                in_col =in_acres_df[str(use)]
                print in_col
                outdf = (in_col/acres_use)*100
                out_matrix_percent[use] =outdf.values
                print outdf
        else:
            pass
    print out_matrix_percent
    csv_name = csv.replace("_acres_","_percent_")
    out_csv_use = out_csv +os.sep +csv_name

    out_matrix_percent.to_csv(out_csv_use)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
