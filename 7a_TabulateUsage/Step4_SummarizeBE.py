import datetime
import os

import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title- Generate BE summary table from master percent overlap tables - this script is interchangeable with the on
# under GAP_Species but kept  separate
#               1) Generates BE summary table at 0, 305 and 792 interval; for aggregated layers, AA, Ag and NonAG
#                   1a) NOTE these interval included the  lower interval; this can be change within script
# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS
# col in UsesLook up that represents the Final Use Header - values do not have

# TODO set up separate script so that it will check for missing runs, right now if there is not datat in the master tables

# ###############user input variables
full_impact = True  # if drift values should include use + drift True if direct use and drift should be separate false
chemical_name = ''
file_type = 'Range' # CriticalHabitat, Range

# out tabulated root path
root_path  = r'out path'
# DO this for no adjust and adjusted tables  #TODO update so it run both un-adjusted and adjusted at the same time

# folder and table names from previous steps
folder_path_no = r'SprayInterval_IntStep_30_MaxDistance_1501\noadjust'
table_no ="R_UnAdjusted_SprayInterval_noadjust_Full Range_20190626.csv"

folder_path_cen = r'SprayInterval_IntStep_30_MaxDistance_1501\census'
table_cen ="R_Lower_SprayInterval_Full Range_census_max_20190626.csv"  # example table will loop through all tables

# COMMENT OUT IF WE ARENT RUNNING HAB _ LIST BELOW ALSO NEED TO CHANGE LINE 40,57, 191, 201
folder_path_hab = r'SprayInterval_IntStep_30_MaxDistance_1501\adjHab'
table_hab ="R_Lower_SprayInterval_Full Range_adjHab_avg_20190626.csv"  # example table will loop through all tables


in_table_path_no = root_path +os.sep+chemical_name+os.sep+file_type+os.sep+folder_path_no
in_table_path_cen = root_path +os.sep+chemical_name+os.sep+file_type+os.sep+folder_path_cen
in_table_path_hab = root_path +os.sep+chemical_name+os.sep+file_type+os.sep+folder_path_hab

# col to include from master
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country','Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

use_lookup_csv = r'path' + os.sep + chemical_name + "Uses_lookup_20190409.csv"


# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

for input_table in [table_no, table_cen, table_hab]:  #HARD CODE ORDER IN THIS LIST and file name TODO UPDATE SO IT IS NOT HARD CODE
    for p_region in ['L48', 'NL48']:
        if input_table.split("_")[1] == 'UnAdjusted':
            in_table = in_table_path_no +os.sep+table_no
            # print in_table
            find_file_type = in_table.split(os.sep)
            if p_region == 'L48':
                # p_region = 'L48'  # can be L48 or
                regions = ['CONUS']
            else:
                regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI', 'AS']
            path_intable, in_table_name = os.path.split(in_table)
            file_type_marker = input_table.split("_")[0]
            usage_group = input_table.split("_")[1]
            acres_group = input_table.split("_")[4]
            temp_folder = path_intable
            suffix = in_table_name.split("_")[2] + "_" + in_table_name.split("_")[3]

            out_csv = temp_folder + os.sep + file_type_marker + "_" + usage_group + "_" + acres_group + '_AllUses_BE_' + p_region + "_" + suffix + "_" + date + '.csv'

            # # update to 792  from 765 3/19/2019
            bins = [0, 305, 792]  # meter conversion of 1000 and 2600 foot buffer round up to the nearest 5

            use_lookup = pd.read_csv(use_lookup_csv)
            use_lookup['FinalColHeader'].fillna('none', inplace=True)
            region_lookup = use_lookup.loc[(use_lookup['Included AA'] == 'x')]

            list_regional_uses = list(set(region_lookup ['FinalColHeader'].values.tolist()))
            # Sets up the intervals that are of interests for each of the uses

            sp_table_df = pd.read_csv(in_table, dtype=object)
            [sp_table_df.drop(m, axis=1, inplace=True) for m in sp_table_df.columns.values.tolist() if m.startswith('Unnamed')]
            drop_acres_nl48 = [m for m in sp_table_df.columns.values.tolist() if m.startswith('TotalAcresNL48')]
            sp_table_df.drop(drop_acres_nl48, axis=1, inplace=True)

            drop_acres_l48 = [m for m in sp_table_df.columns.values.tolist() if m.startswith('Acres_')]
            sp_table_df.drop(drop_acres_l48, axis=1, inplace=True)

            drop_acres_all = [m for m in sp_table_df.columns.values.tolist() if m.startswith('TotalAcresOnLand')]
            sp_table_df.drop(drop_acres_all, axis=1, inplace=True)

            columns_uses =[t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] in regions]

            columns_species = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] not in ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI', 'AS','CONUS']]
            # print sp_table_df
            sp_info_df = sp_table_df.loc[:, columns_species]
            use_df = sp_table_df.loc[:, columns_uses]

            use_list = use_df.columns.values.tolist()
            uses = []
            for x in use_list:
                split_value = x.split("_")
                interval_value = split_value[len(split_value) - 1]
                use_nm = split_value[1]
                for v in split_value:
                    if v in regions or v == use_nm or v == interval_value:
                        pass
                    else:
                        use_nm = use_nm + v
                uses.append(use_nm)

            collapsed_df = pd.DataFrame()

            for i in list_regional_uses:

                # NOTES SEE ASSUMPTION ABOUT NOT HAVING ANY "_" in the use name part of final use col headers;
                # ie [region]_[use name]_[intervalvalue] list of current group of column will not populate correctly
                break_use = i.split("_")
                use_group = break_use[0] + "_" + break_use[1]

                current_group = [use for use in use_list if (use.split("_")[0] + "_" + use.split("_")[1]) == use_group]

                if len(current_group) == 0:
                    continue
                grouped_use = use_df.loc[:, current_group]

                current_cols = grouped_use.columns.values.tolist()
                previous_col = []

                for value in bins:

                    new_df = pd.DataFrame()
                    binned_col = []
                    if full_impact:  # direct use is included is drift calculations
                        for col in current_cols:
                            get_interval = col.split('_')
                            interval = int(get_interval[(len(get_interval) - 1)])
                            if interval <= value and col not in previous_col:
                                binned_col.append(col)

                    elif not full_impact:  # direct use is not included is drift calculations

                        if value == bins[0]:
                            for col in current_cols:
                                get_interval = col.split('_')
                                interval = int(get_interval[(len(get_interval) - 1)])
                                if interval == bins[0]:

                                    binned_col.append(col)
                        else:

                            for col in current_cols:
                                get_interval = col.split('_')
                                interval = int(get_interval[(len(get_interval) - 1)])
                                if interval == 0:
                                    continue
                                else:
                                    if interval <= value and col not in previous_col:
                                        binned_col.append(col)

                        for p in binned_col:
                            if p in previous_col:
                                binned_col.remove(p)

                            previous_col.append(p)

                    binned_df = grouped_use[binned_col]

                    use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
                    new_df[(str(use_group) + '_' + str(value))] = use_results_df.sum(axis=1)

                    collapsed_df = pd.concat([collapsed_df, new_df], axis=1)


            final_df = pd.concat([sp_info_df, collapsed_df], axis=1)
            col_final = collapsed_df.columns.values.tolist()
            master_col = col_include_output
            for i in col_final:
                master_col.append(i)

            # final_df = final_df.reindex(columns=master_col)
            final_df.to_csv(out_csv)
            print out_csv
        else:
            index_pos= [table_no, table_cen, table_hab].index(input_table)
            # index_pos= [table_no, table_cen].index(input_table)
            for group in ['min','max','avg']:
                for bound in ['Lower', 'Upper', 'Uniform']:
                    # input_table.split("_")
                    table= input_table.split("_")[0] +"_"+bound+"_"+input_table.split("_")[2]+"_"+input_table.split("_")[3]+"_"+input_table.split("_")[4]+"_"+group+"_"+input_table.split("_")[6]
                    if index_pos == 1:
                        in_table = in_table_path_cen+os.sep+table
                    else:
                        in_table = in_table_path_hab+os.sep+table
                    # print in_table
                    find_file_type = in_table.split(os.sep)
                    if p_region == 'L48':
                        # p_region = 'L48'  # can be L48 or
                        regions = ['CONUS']
                    else:
                        regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI', 'AS']
                    path_intable, in_table_name = os.path.split(in_table)
                    file_type_marker = table.split("_")[0]
                    usage_group = table.split("_")[1]
                    acres_group = table.split("_")[4]
                    temp_folder = path_intable
                    suffix = in_table_name.split("_")[2] + "_" + in_table_name.split("_")[3] + "_" + in_table_name.split("_")[5]

                    out_csv = temp_folder + os.sep + file_type_marker + "_" + usage_group + "_" + acres_group + '_AllUses_BE_' + p_region + "_" + suffix + "_" + date + '.csv'

                    # # update to 792  from 765 3/19/2019
                    bins = [0, 305, 792]  # meter conversion of 1000 and 2600 foot buffer round up to the nearest 5

                    use_lookup = pd.read_csv(use_lookup_csv)
                    use_lookup['FinalColHeader'].fillna('none', inplace=True)
                    region_lookup = use_lookup.loc[(use_lookup['Included AA'] == 'x')]

                    list_regional_uses = list(set(region_lookup ['FinalColHeader'].values.tolist()))

                    # Sets up the intervals that are of interests for each of the uses

                    sp_table_df = pd.read_csv(in_table, dtype=object)
                    [sp_table_df.drop(m, axis=1, inplace=True) for m in sp_table_df.columns.values.tolist() if m.startswith('Unnamed')]
                    drop_acres_nl48 = [m for m in sp_table_df.columns.values.tolist() if m.startswith('TotalAcresNL48')]
                    sp_table_df.drop(drop_acres_nl48, axis=1, inplace=True)

                    drop_acres_l48 = [m for m in sp_table_df.columns.values.tolist() if m.startswith('Acres_')]
                    sp_table_df.drop(drop_acres_l48, axis=1, inplace=True)

                    drop_acres_all = [m for m in sp_table_df.columns.values.tolist() if m.startswith('TotalAcresOnLand')]
                    sp_table_df.drop(drop_acres_all, axis=1, inplace=True)

                    columns_uses =[t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] in regions]

                    columns_species = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] not in ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI', 'AS','CONUS']]
                    # print sp_table_df
                    sp_info_df = sp_table_df.loc[:, columns_species]
                    use_df = sp_table_df.loc[:, columns_uses]

                    use_list = use_df.columns.values.tolist()


                    uses = []
                    for x in use_list:
                        split_value = x.split("_")
                        interval_value = split_value[len(split_value) - 1]
                        use_nm = split_value[1]
                        for v in split_value:
                            if v in regions or v == use_nm or v == interval_value:
                                pass
                            else:
                                use_nm = use_nm + v
                        uses.append(use_nm)

                    collapsed_df = pd.DataFrame()

                    for i in list_regional_uses:

                            # NOTES SEE ASSUMPTION ABOUT NOT HAVING ANY "_" in the use name part of final use col headers;
                            # ie [region]_[use name]_[intervalvalue] list of current group of column will not populate correctly
                            break_use = i.split("_")
                            use_group = break_use[0] + "_" + break_use[1]

                            current_group = [use for use in use_list if (use.split("_")[0] + "_" + use.split("_")[1]) == use_group]

                            if len(current_group) == 0:
                                continue
                            grouped_use = use_df.loc[:, current_group]

                            current_cols = grouped_use.columns.values.tolist()
                            previous_col = []

                            for value in bins:

                                new_df = pd.DataFrame()
                                binned_col = []
                                if full_impact:  # direct use is included is drift calculations
                                    for col in current_cols:
                                        get_interval = col.split('_')
                                        interval = int(get_interval[(len(get_interval) - 1)])
                                        if interval <= value and col not in previous_col:
                                            binned_col.append(col)

                                elif not full_impact:  # direct use is not included is drift calculations

                                    if value == bins[0]:
                                        for col in current_cols:
                                            get_interval = col.split('_')
                                            interval = int(get_interval[(len(get_interval) - 1)])
                                            if interval == bins[0]:

                                                binned_col.append(col)
                                    else:

                                        for col in current_cols:
                                            get_interval = col.split('_')
                                            interval = int(get_interval[(len(get_interval) - 1)])
                                            if interval == 0:
                                                continue
                                            else:
                                                if interval <= value and col not in previous_col:
                                                    binned_col.append(col)

                                    for p in binned_col:
                                        if p in previous_col:
                                            binned_col.remove(p)

                                        previous_col.append(p)

                                binned_df = grouped_use[binned_col]

                                use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
                                new_df[(str(use_group) + '_' + str(value))] = use_results_df.sum(axis=1)

                                collapsed_df = pd.concat([collapsed_df, new_df], axis=1)


                    final_df = pd.concat([sp_info_df, collapsed_df], axis=1)
                    col_final = collapsed_df.columns.values.tolist()
                    master_col = col_include_output
                    for i in col_final:
                        master_col.append(i)

                    # final_df = final_df.reindex(columns=master_col)
                    final_df.to_csv(out_csv)
                    print out_csv

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
