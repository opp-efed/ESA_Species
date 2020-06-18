import pandas as pd
import os

aa_col_header = 'Carbaryl AA'
aaag_col_header= 'Carbaryl AA Ag'
aanonag_col_header= 'Carbaryl AA NonAg'
out_table= 'R_Upper_max.csv'

aa_col = ['CONUS_Carbaryl AA_0', 'NL48_Carbaryl AA_0']
aa_ag_noag= ['NL48_Carbaryl AA NonAg_0', 'NL48_Carbaryl AA Ag_0','CONUS_Carbaryl AA NonAg_0', 'CONUS_Carbaryl AA Ag_0']
#
step1_gis = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Carbaryl\Summarized Tables\Step 1\GIS_Step1_R_Carbaryl.csv"
step2_gis =r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Carbaryl\Summarized Tables\5_On_Off_Field_HAB\max\Upper\R_On_Off_Field_GIS_Step2_Carbaryl.csv"
ch = False
# step1_gis = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Carbaryl\Summarized Tables\Step 1\GIS_Step1_CH_Carbaryl.csv"
# step2_gis =r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Carbaryl\Summarized Tables\4_On_Off_Field\max\Upper\CH_On_Off_Field_GIS_Step2_Carbaryl.csv"
# ch = True
out_location = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\result_summary'

out_df = pd.DataFrame(columns=['Step 1', 'Step 1 Total', 'Step 2', 'Step 2 Total'])
step1_df = pd.read_csv(step1_gis)
step2_df = pd.read_csv(step2_gis)
if ch:
    step1_df = step1_df[(step1_df['Des_CH']=='TRUE')]
    step2_df = step2_df[(step2_df['Des_CH']=='TRUE')]

ne_conus = len(step1_df[(step1_df['CONUS_'+ aa_col_header+"_"+'792']==0) & (step1_df['Step 1 ED Comment']!='Qualitative') & (step1_df['L48/NL48']=='CONUS')])
ne_nl48 = len(step1_df[(step1_df['NL48_'+ aa_col_header+"_"+'792']==0) & (step1_df['Step 1 ED Comment']!='Qualitative') & (step1_df['L48/NL48']=='NL48')])
nlaa_conus = len(step1_df[(step1_df['CONUS_'+ aa_col_header+"_"+'792']<0.44) &(step1_df['CONUS_'+ aa_col_header+"_"+'792']>0) & (step1_df['Step 1 ED Comment']!='Qualitative') & (step1_df['L48/NL48']=='CONUS')])
nlaa_nl48 = len(step1_df[(step1_df['NL48_'+ aa_col_header+"_"+'792']<0.44) &(step1_df['NL48_'+ aa_col_header+"_"+'792']>0) & (step1_df['Step 1 ED Comment']!='Qualitative') & (step1_df['L48/NL48']=='NL48')])
fed_lands = len(step1_df[(step1_df['CONUS_Federal Lands_0']>98.44) | (step1_df['NL48_Federal Lands_0']>98.44) & (step1_df['Step 1 ED Comment']!='No Effect - Overlap')])
qual = len(step1_df[(step1_df['Step 1 ED Comment']=='Qualitative')])

aa_five_conus = len(step2_df[(step2_df['CONUS_'+ aa_col_header+"_"+'792']<4.44)& (step2_df['CONUS_'+ aa_col_header+"_"+'792']>0.44)&
                       (step2_df['Step 2 ED Comment']!='Qualitative') & (step2_df['Step 2 ED Comment']!='No Effect - Federal Land')&(step2_df['L48/NL48']=='CONUS')])
aa_five_nl48 = len(step2_df[(step2_df['NL48_'+ aa_col_header+"_"+'792']<4.44)& (step2_df['NL48_'+ aa_col_header+"_"+'792']>0.44)&
                            (step2_df['Step 2 ED Comment']!='Qualitative') & (step2_df['Step 2 ED Comment']!='No Effect - Federal Land')&(step2_df['L48/NL48']=='NL48')])
fed_lands_five = len(step2_df[(((step2_df['CONUS_Federal Lands_0']<98.45) & (step2_df['CONUS_Federal Lands_0']>94.44))
                               |((step2_df['NL48_Federal Lands_0']<98.45) & (step2_df['NL48_Federal Lands_0']>94.44)))&((step2_df['Step 2 ED Comment']!='NLAA Overlap - 1percent')&(step2_df['Step 2 ED Comment']!='NLAA Overlap - 5percent'))])
list_uses_direct = [col for col in step2_df.columns.values.tolist() if col.endswith("_0")]


list_uses_direct = [col for col in list_uses_direct if col not in aa_col and col not in aa_ag_noag]
for col in list_uses_direct:
    if col in aa_col:
        print col
        list_uses_direct.remove(col)
    aa_check = col.split("_")
    if 'Federal Lands' in aa_check:
        list_uses_direct.remove(col)

for col in aa_ag_noag:
    if col in list_uses_direct:
        list_uses_direct.remove(col)

print list_uses_direct
step2_df['Sum Direct'] = step2_df[list_uses_direct].sum(axis=1)
step2_df['Max Direct'] = step2_df[list_uses_direct].max(axis=1)

nlaa_overlap = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') & (step2_df['CONUS_'+ aa_col_header+"_"+'792']>4.44)& (step2_df['Max Direct']<0.44)&(step2_df['L48/NL48']=='CONUS')])
less_one = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') &(step2_df['Sum Direct']<0.44)&(step2_df['L48/NL48']=='CONUS')])
one= len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') &(step2_df['Max Direct']>0.44) &(step2_df['Sum Direct']>0.44)&(step2_df['Sum Direct']<1.44)&(step2_df['L48/NL48']=='CONUS')])
five= len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')& (step2_df['Sum Direct']>1.44)&(step2_df['Sum Direct']<5.44)&(step2_df['L48/NL48']=='CONUS')])
ten = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')& (step2_df['Sum Direct']>5.44)&(step2_df['Sum Direct']<10.44)&(step2_df['L48/NL48']=='CONUS')])
fifteen = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') & (step2_df['Sum Direct']>10.44)&(step2_df['Sum Direct']<15.44)&(step2_df['L48/NL48']=='CONUS')])
twnty = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')& (step2_df['Sum Direct']>15.44)&(step2_df['Sum Direct']<20.44)&(step2_df['L48/NL48']=='CONUS')])
g_twnty =len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')& (step2_df['Sum Direct']>20.44)&(step2_df['L48/NL48']=='CONUS')])

nlaa_overlap_nl = len(step2_df[ (step2_df['Step 2 ED Comment']=='May Affect')& (step2_df['NL48_'+ aa_col_header+"_"+'792']>4.44) &(step2_df['Max Direct']<0.44)&(step2_df['L48/NL48']=='NL48')])
less_one_nl = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') &(step2_df['Sum Direct']<0.44)&(step2_df['L48/NL48']=='NL48')])
one_nl= len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') &  (step2_df['Sum Direct']>0.44)&(step2_df['Sum Direct']<1.44)&(step2_df['L48/NL48']=='NL48')])
five_nl= len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') & (step2_df['Sum Direct']>1.44)&(step2_df['Sum Direct']<5.44)&(step2_df['L48/NL48']=='NL48')])
ten_nl = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')&  (step2_df['Sum Direct']>5.44)&(step2_df['Sum Direct']<10.44)&(step2_df['L48/NL48']=='NL48')])
fifteen_nl = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')&  (step2_df['Sum Direct']>10.44)&(step2_df['Sum Direct']<15.44)&(step2_df['L48/NL48']=='NL48')])
twnty_nl = len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect')&  (step2_df['Sum Direct']>15.44)&(step2_df['Sum Direct']<20.44)&(step2_df['L48/NL48']=='NL48')])
g_twnty_nl =len(step2_df[(step2_df['Step 2 ED Comment']=='May Affect') & (step2_df['Sum Direct']>20.44)&(step2_df['L48/NL48']=='NL48')])


# List of series
listOfSeries = [pd.Series(['0 Overlap - CONUS', ne_conus, '5% AA CONUS', aa_five_conus ], index=out_df.columns ),
                pd.Series(['0 Overlap - NL48', ne_nl48, '5% AA NL48', aa_five_nl48], index=out_df.columns)  ,
                pd.Series(['1% Overlap - CONUS', nlaa_conus, 'Federal Lands 95%', fed_lands_five], index=out_df.columns ),
                pd.Series(['1% Overlap - NL48', nlaa_nl48, 'NLAA overlap -CONUS', nlaa_overlap], index=out_df.columns)  ,
                pd.Series(['Qualitative', qual, 'NLAA overlap -NL48', nlaa_overlap_nl], index=out_df.columns),
                pd.Series(['Federal Lands 99%', fed_lands, '<1% Overlap-CONUS', less_one], index=out_df.columns),

                pd.Series(['', '', '1% Overlap-CONUS', one], index=out_df.columns),
                pd.Series(['', '', '5% Overlap-CONUS', five], index=out_df.columns),
                pd.Series(['', '', '10% Overlap-CONUS', ten], index=out_df.columns),
                pd.Series(['', '', '15% Overlap-CONUS', fifteen], index=out_df.columns),
                pd.Series(['', '', '20% Overlap-CONUS', twnty], index=out_df.columns),
                pd.Series(['', '', '>20% Overlap-CONUS', g_twnty], index=out_df.columns),
                pd.Series(['', '', '<1% Overlap-NL48', less_one_nl], index=out_df.columns),
                pd.Series(['', '', '1% Overlap-NL48', one_nl], index=out_df.columns),
                pd.Series(['', '', '5% Overlap-NL48', five_nl], index=out_df.columns),
                pd.Series(['', '', '10% Overlap-NL48', ten_nl], index=out_df.columns),
                pd.Series(['', '', '15% Overlap-NL48', fifteen_nl], index=out_df.columns),
                pd.Series(['', '', '20% Overlap-NL48', twnty_nl], index=out_df.columns),
                pd.Series(['', '', '>20% Overlap-NL48', g_twnty_nl], index=out_df.columns),

                ]

# Pass a list of series to the append() to add multiple rows
out_df = out_df.append(listOfSeries , ignore_index=True)
out_df.to_csv(out_location +os.sep+ out_table)

# nlaa_overlap_nl = len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Max Direct']<0.44)&(step2_df['L48/NL48']=='NL48')])
# less_one_nl = len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']<0.44)&(step2_df['L48/NL48']=='NL48')])
# one_nl= len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']>0.44)&(step2_df['Sum Direct']<1.44)&(step2_df['L48/NL48']=='NL48')])
# five_nl= len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']>1.44)&(step2_df['Sum Direct']<5.44)&(step2_df['L48/NL48']=='NL48')])
# ten_nl = len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']>5.44)&(step2_df['Sum Direct']<10.44)&(step2_df['L48/NL48']=='NL48')])
# fifteen_nl = len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']>10.44)&(step2_df['Sum Direct']<15.44)&(step2_df['L48/NL48']=='NL48')])
# twnty_nl = len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']>15.44)&(step2_df['Sum Direct']<20.44)&(step2_df['L48/NL48']=='NL48')])
# g_twnty_nl =len(step2_df[((step2_df['Step 2 ED Comment']=='May Affect')|(step2_df['Step 2 ED Comment']=='NLAA - Overlap - 5percent')) & (step2_df['Sum Direct']>20.44)&(step2_df['L48/NL48']=='NL48')])
