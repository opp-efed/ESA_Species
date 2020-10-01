import pandas as pd
import numpy as np
import os


out_path_combined = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Test_Overlap summary combined_onoff_wcntysp_old'

list_csv = os.listdir(out_path_combined)
species_summary = [v for v in list_csv if v.startswith('Species')]
county_summary = [ v.replace('Species_',"") for v in species_summary]
bins = ['_0', '_30', '_60', '_90','_120', '_150','_180','_210','_240']
list_pol_ids = ['AL', 'AR', 'AZ', 'CO', 'DE', 'FL', 'GA', 'IA', 'IL', 'IN', 'KS', 'KY', 'LA', 'MD', 'MI', 'MN', 'MO',
                'MS', 'NC','ND', 'NE', 'NJ', 'NM', 'NY', 'OH', 'OK', 'PA', 'SC', 'SD', 'TN', 'TX', 'VA', 'WI', 'WV']

registration_States= pd.DataFrame(data=list_pol_ids, columns=['STUSPS_x'])
groups_of_results = []
for table in county_summary:
    split_name = table.split("_")
    length = len(split_name)
    group = table.replace("_"+split_name[length-1],'')
    if group  not in groups_of_results:
        groups_of_results.append(group)

for group in groups_of_results:

    for interval in bins:
        cnty_table = group +interval+'.csv'
        cnty = pd.read_csv(out_path_combined +os.sep+cnty_table)
        cnty_in_state = cnty[['GEOID','STUSPS_x', 'County confidence']].copy()
        cnty_in_state = cnty_in_state[cnty_in_state['County confidence']=='Include'].copy()
        cnty_in_state.drop_duplicates(inplace=True)
        cnt_counties=cnty_in_state['STUSPS_x'].value_counts().reset_index()
        cnt_counties.columns = ['STUSPS_x', 'Number of Confident Counties in State']

        cnt_counties = pd.merge(registration_States, cnt_counties, how='left',on='STUSPS_x')
        cnt_counties.fillna(0,inplace=True)

        percent_calc = cnty[['GEOID','STUSPS_x','COTTON','COTTON_Total','SOYBEANS','SOYBEAN_Total',
                                 'Percent of Total UDL Soybean','Percent of Total UDL Cotton', 'County confidence']].copy()

        percent_calc  = percent_calc.groupby(['GEOID','STUSPS_x','COTTON','COTTON_Total','SOYBEANS','SOYBEAN_Total','County confidence'], as_index=False).max().reset_index()
        cnty_for_grouping = percent_calc[['STUSPS_x','COTTON','SOYBEANS','COTTON_Total','SOYBEAN_Total','County confidence','Percent of Total UDL Soybean','Percent of Total UDL Cotton']]
        cnty_for_grouping = cnty_for_grouping[cnty_for_grouping['County confidence']=='Include'].copy()
        cnty_for_grouping['Adjusted COTTON'] = (cnty_for_grouping ['COTTON']* (cnty_for_grouping ['Percent of Total UDL Cotton']/100))
        cnty_for_grouping['Adjusted SOYBEANS'] = (cnty_for_grouping ['SOYBEANS']* (cnty_for_grouping ['Percent of Total UDL Soybean']/100))
        totals = cnty_for_grouping[['STUSPS_x','COTTON_Total','SOYBEAN_Total']]
        totals.drop_duplicates(inplace=True)

        cnty_for_grouping= cnty_for_grouping[['STUSPS_x','COTTON','SOYBEANS','Adjusted COTTON','Adjusted SOYBEANS']]

                # cnty_for_grouping.drop('County confidence',axis=1, inplace= True)
        cnty_for_grouping.drop_duplicates(inplace=True)

        grouped = cnty_for_grouping.groupby('STUSPS_x').sum().reset_index()
        grouped = pd.merge(grouped,totals, how='right', on='STUSPS_x')
        grouped ['Percent Cotton Impacted in State'] = (grouped['COTTON']/grouped['COTTON_Total'])*100
        grouped ['Percent Soybeans Impacted in State'] = (grouped['SOYBEANS']/grouped['SOYBEAN_Total'])*100
        grouped ['Adjusted Percent Cotton Impacted in State'] = (grouped['Adjusted COTTON']/grouped['COTTON_Total'])*100
        grouped ['Adjusted Percent Soybeans Impacted in State'] = (grouped['Adjusted SOYBEANS']/grouped['SOYBEAN_Total'])*100
        grouped = pd.merge(grouped,cnt_counties, how='outer', on='STUSPS_x')
        grouped.fillna(0,inplace=True)
        grouped.to_csv (out_path_combined +os.sep+"State_Summary_"+cnty_table)


        #     # print ('Cnty table is {0} and Species table is {1}'.format(cnty_table,'Species_'+cnty_table))
    #
    #     per_impact_cotton = (cnty['Percent Impact Cotton'].unique())
    #     per_impact_cotton = per_impact_cotton[~np.isnan(per_impact_cotton)]
    #     per_impact_cotton = per_impact_cotton[0]
    #
    #     per_impact_soybeans  = (cnty['Percent Impact Soybean'].unique())
    #     per_impact_soybeans  = per_impact_soybeans[~np.isnan(per_impact_soybeans)]
    #     per_impact_soybeans  = per_impact_soybeans[0]
    #
    #     cnty_cnt  = (cnty['County Count'].unique())
    #     cnty_cnt  = cnty_cnt[~np.isnan(cnty_cnt)]
    #     cnty_cnt = cnty_cnt[0]
    #
    #     confident_cnty_cnt   = (cnty ['Confident County Count'].unique())
    #     confident_cnty_cnt  = confident_cnty_cnt [~np.isnan(confident_cnty_cnt )]
    #     confident_cnty_cnt  = confident_cnty_cnt [0]
    #
    #     species_cnt = species['Species Count'].iloc[0]
    #     ch_cnt = species['Critical Habitat Count'].iloc[0]
    #     sp_ch = species['EntityID'].nunique()
    #     meters = interval.replace("_","")
    #     out_table=out_table.append({'meters': meters, 'Number of counties':cnty_cnt,'Number of confident counties':confident_cnty_cnt ,'Number of Species': species_cnt,'Number of Critical habitats': ch_cnt,
    #                                 'Combined Species and Critical habitat': sp_ch ,	'Percent of Cotton Impacted CoA 2012':per_impact_cotton ,
    #                                 'Percent of Soybean Impacted CoA 2012':per_impact_soybeans},ignore_index=True)
    # out_table['marginal counties'] =  out_table['Number of counties'] -out_table['Number of confident counties']
    # out_table.to_csv(out_path_combined +os.sep+ "Summary_"+ group +'.csv')
    # # print cnty_cnt, per_impact_cotton,per_impact_soybeans, species_cnt, ch_cnt, sp_ch
    #
    #
