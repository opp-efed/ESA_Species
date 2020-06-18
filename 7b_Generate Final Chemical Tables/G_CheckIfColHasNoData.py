import os
import pandas as pd


chemical = 'Methomyl'  # Methomyl Carbaryl
sub_folder = r"Summarized Tables\3_Redundancy"

path= r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables' +os.sep+ chemical+os.sep+ sub_folder
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']
get_folder= os.listdir(path)


for root,dirs,files in os.walk(path, topdown=False):
    for name in files:
        file_name =(os.path.join(root,name))
        if file_name.endswith('.csv'):
            if os.path.exists(file_name):
                df = pd.read_csv(file_name)
                col= df.columns.values.tolist()
                col_to_check = [v for v in col if v not in col_include_output]
                # print col_to_check
                for i in col_to_check:
                    total = df[i].sum()
                    if total ==0:
                        print ("Check table {0} col {1}: values sum to 0".format(os.path.basename(file_name), i))

    for name in dirs:
        file_name =(os.path.join(root,name))
        if file_name.endswith('.csv'):
            if os.path.exists(file_name):
                df = pd.read_csv(file_name)
                col= df.columns.values.tolist()
                col_to_check = [v for v in col if v not in col_include_output]
                # print col_to_check
                for i in col_to_check:
                    total = df[i].sum()
                    if total ==0:
                        print ("Check table {0} col {1}: values sum to 0".format(os.path.basename(file_name), i))