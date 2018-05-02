import pandas as pd
import os

in_file_structure ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SharePoint\Guid_Doc_Team\G_driveHierarchy.csv'
out_location = 'G:\Guidance Document Review'

file_df = pd.read_csv(in_file_structure)
count_row = len(file_df)-1
start_row = 0
while start_row <count_row:
    row_series = file_df.iloc[start_row]
    row_series.dropna(inplace =True)
    previous_path = out_location
    for i in row_series:
        c_path = previous_path +os.sep+i

        if not os.path.exists(c_path):
            os.mkdir(c_path)
            previous_path = c_path
        else:
            previous_path =c_path
    start_row +=1

