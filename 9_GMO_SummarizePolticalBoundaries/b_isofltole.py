import pandas as pd
import os

reg = pd.read_excel(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\isoxaflutole\IFT Soy label states and counties.xlsx')
range_df = pd.read_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\isoxaflutole\EPA_pivot_noplant.csv')
list_st = range_df.columns.values.tolist()
print list_st
list_st.remove('Unnamed: 0')


out_df_range = pd.DataFrame(index=(list(range(0, 700))),columns = list_st)
out_df_reg = pd.DataFrame(index=(list(range(0, 700))), columns = list_st)

for v in list_st:
    print v
    in_range_not_reg = []
    in_reg_not_range = []

    reg_cnty = reg[v].values.tolist()
    range_cnty = range_df[v].values.tolist()

    for i in reg_cnty:
        if i not in range_cnty:
            in_reg_not_range.append(i)
        else:
            pass
    count = len(in_reg_not_range)
    remaining = [None] * (700 - count)  # all columns need to 1000 rows - makes additional rows with value none
    in_reg_not_range = in_reg_not_range + remaining
    series_reg = pd.Series(in_reg_not_range)
    out_df_reg[v] = series_reg.values

    for j in range_cnty:
        if j not in reg_cnty:
            in_range_not_reg.append(j)
        else:
            pass
    count = len(in_range_not_reg)

    remaining_range = [None] * (700 - count)  # all columns need to 1000 rows - makes additional rows with value none
    in_range_not_reg = in_range_not_reg + remaining_range
    series_range= pd.Series(in_range_not_reg)

    out_df_range[v] = series_range.values

out_df_reg.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\isoxaflutole\InReg_notRange.csv')
out_df_range.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\isoxaflutole\InRange_notReg.csv')