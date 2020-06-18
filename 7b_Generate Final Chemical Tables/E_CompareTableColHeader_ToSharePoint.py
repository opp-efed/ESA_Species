import pandas as pd
import os


chemical = 'Carbaryl'  # Methomyl Carbaryl
sub_folder = r"\Summarized Tables\4_On_Off_Field"
sharepoint_path = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables' +os.sep+ chemical+os.sep+ sub_folder
update_path= r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCAB' +os.sep+ chemical+os.sep+ sub_folder

get_folder= os.listdir(sharepoint_path)

for root,dirs,files in os.walk(sharepoint_path, topdown=False):
    for name in files:
        file_name =(os.path.join(root,name))
        if file_name.endswith('.csv'):
            update_file = file_name.replace(sharepoint_path,update_path)
            if os.path.exists(update_file):
                share_df = pd.read_csv(file_name)
                update_df = pd.read_csv(update_file)
                col_share = share_df.columns.values.tolist()
                col_update = update_df.columns.values.tolist()
                # if col_share.sort() == col_update.sort():
                if col_share == col_update:
                    print("Print cols are identical")
                else:
                    print update_file
                    print file_name
                    print("Print cols are not identical")
                    print (list(set(col_share) - set(col_update)))
            else:
                print ("{0} file is not in update".format(update_file))

    for name in dirs:
        file_name =(os.path.join(root,name))
        if file_name.endswith('.csv'):
            update_file = file_name.replace(sharepoint_path,update_path)
            if os.path.exists(update_file):
                share_df = pd.read_csv(file_name)
                update_df = pd.read_csv(update_file)
                col_share = share_df.columns.values.tolist()
                col_update = update_df.columns.values.tolist()
                if col_share == col_update:
                    print("Print cols are identical")
                else:
                    print update_file
                    print("Print cols are not identical")
                    print (list(set(col_share) - set(col_update)))
            else:
                print ("{0} file is not in update".format(update_file) )