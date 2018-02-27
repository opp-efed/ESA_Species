import os
import pandas as pd
import shutil
wpd_out = r"J:\wpd files"
dup_out = r'J:\dup files'
triple_files = r'J:\triple files'
inpath = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SharePoint\WPD_Files_G_GuidanceFolder.csv"

in_files= pd.read_csv(inpath)

path = in_files['full path'].values.tolist()

for v in path:
    path,filename = os.path.split(v)
    if os.path.exists(wpd_out +os.sep+filename ):
        if os.path.exists(dup_out+os.sep+filename ):
            shutil.copy(v, triple_files)
        else:
            shutil.copy(v, dup_out)
    else:
        shutil.copy(v, wpd_out)


