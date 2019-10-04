import pandas as pd
import os
import datetime

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# look ups from county and HUC
look_up_fcs = r'path\Lookup_R_Clipped_Union_CntyInter_HUC2ABInter_[date]'
# files combine with additonal data from step 4
in_combine_grids = r'path\Grid_byProjections_Combined'
# outlocation for lookup
out_location = r'outpath\LookUp_Grid_byProjections_Combined'

list_dir = os.listdir(in_combine_grids)
list_huc_csv = os.listdir(look_up_fcs)

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

for folder in list_dir:
    if not os.path.exists(out_location + os.sep + folder):
        os.mkdir(out_location + os.sep + folder)
    list_grid = os.listdir(in_combine_grids + os.sep + folder)
    list_grid_att = [csv for csv in list_grid if csv.endswith('att.csv')]
    for c_grid in list_grid_att:
        if not os.path.exists(out_location + os.sep + folder + os.sep + c_grid):
            grid = pd.read_csv(in_combine_grids + os.sep + folder + os.sep + c_grid)
            col_header = pd.read_csv(in_combine_grids + os.sep + folder +os.sep + c_grid.split("_")[0]+ "_"+c_grid.split("_")[1]+ '_lookup_rasters.csv')
            # makes a list of the current col headers, and if they need be updated based on the look up table
            # then makes the update
            spe_col = grid.columns.values.tolist()
            update_col = []

            for col in spe_col:
                if col in col_header['Default output header'].values.tolist():
                    new_col = \
                        col_header.loc[
                            col_header['Default output header'] == col, 'Desired output header'].iloc[
                            0]
                    update_col.append(new_col)
                else:
                    update_col.append(col)
            grid.columns = update_col

            merge_col = [c for c in grid.columns.values.tolist() if c.startswith('r') or c.startswith('ch')]

            sp_fc = [v for v in list_huc_csv if
                     v.startswith(c_grid.split("_")[0].upper() + "_" + c_grid.split("_")[1].capitalize())]
            species_information = pd.read_csv(look_up_fcs + os.sep + sp_fc[0])

            merged_zone_df = merged_df = pd.merge(grid, species_information, left_on=merge_col[0], right_on='HUCID',
                                                  how='left')

            merged_zone_df['HUCID'] = merged_zone_df['HUCID'].map(lambda x: str(x).split('.')[0]).astype(str)
            merged_zone_df['ZoneID'] = merged_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
            merged_zone_df[merge_col[0]] = merged_zone_df[merge_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
            [merged_zone_df.drop(col, axis=1, inplace=True) for col in merged_zone_df.columns.values.tolist() if
             col.startswith('Un')]
            merged_zone_df.to_csv(out_location + os.sep + folder + os.sep + c_grid)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
