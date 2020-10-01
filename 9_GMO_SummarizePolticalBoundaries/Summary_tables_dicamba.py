import pandas as pd
import numpy as np
import os


out_path_combined = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap summary combined_onoff_wcnty'

list_csv = os.listdir(out_path_combined)
species_summary = [v for v in list_csv if v.startswith('Species')]
county_summary = [ v.replace('Species_',"") for v in species_summary]
bins = ['_0', '_30', '_60', '_90','_120', '_150','_180','_210','_240']

groups_of_results = []
for table in county_summary:
    split_name = table.split("_")
    length = len(split_name)
    group = table.replace("_"+split_name[length-1],'')
    if group  not in groups_of_results:
        groups_of_results.append(group)

for group in groups_of_results:
    out_table = pd.DataFrame(columns=['meters','Number of counties','Number of confident counties','Number of Species','Number of Critical habitats',
                                      'Combined Species and Critical habitat',	'Percent of Cotton Impacted CoA 2012',
                                      'Percent of Soybean Impacted CoA 2012'])
    for interval in bins:
        cnty_table = group +interval+'.csv'
        cnty = pd.read_csv(out_path_combined +os.sep+cnty_table)
        species = pd.read_csv(out_path_combined+os.sep+'Species_'+cnty_table)
        # print ('Cnty table is {0} and Species table is {1}'.format(cnty_table,'Species_'+cnty_table))

        per_impact_cotton = (cnty['Percent Impact Cotton'].unique())
        per_impact_cotton = per_impact_cotton[~np.isnan(per_impact_cotton)]
        per_impact_cotton = per_impact_cotton[0]

        per_impact_soybeans  = (cnty['Percent Impact Soybean'].unique())
        per_impact_soybeans  = per_impact_soybeans[~np.isnan(per_impact_soybeans)]
        per_impact_soybeans  = per_impact_soybeans[0]

        cnty_cnt  = (cnty['County Count'].unique())
        cnty_cnt  = cnty_cnt[~np.isnan(cnty_cnt)]
        cnty_cnt = cnty_cnt[0]

        confident_cnty_cnt   = (cnty ['Confident County Count'].unique())
        confident_cnty_cnt  = confident_cnty_cnt [~np.isnan(confident_cnty_cnt )]
        confident_cnty_cnt  = confident_cnty_cnt [0]

        species_cnt = species['Species Count'].iloc[0]
        ch_cnt = species['Critical Habitat Count'].iloc[0]
        sp_ch = species['EntityID'].nunique()
        meters = interval.replace("_","")
        out_table=out_table.append({'meters': meters, 'Number of counties':cnty_cnt,'Number of confident counties':confident_cnty_cnt ,'Number of Species': species_cnt,'Number of Critical habitats': ch_cnt,
                         'Combined Species and Critical habitat': sp_ch ,	'Percent of Cotton Impacted CoA 2012':per_impact_cotton ,
                         'Percent of Soybean Impacted CoA 2012':per_impact_soybeans},ignore_index=True)
    out_table['marginal counties'] =  out_table['Number of counties'] -out_table['Number of confident counties']
    out_table.to_csv(out_path_combined +os.sep+ "Summary_"+ group +'.csv')
        # print cnty_cnt, per_impact_cotton,per_impact_soybeans, species_cnt, ch_cnt, sp_ch


