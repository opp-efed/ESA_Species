import datetime
import os
import arcpy

import pandas as pd

# Internal deliberative, do not cite or distribute
# Author J. Connolly


#out folder location
outlocation = r'D:\Species\Species Spatial Library\_CurrentFiles'
# out files name as a .csv
outfile_nm = 'Range_MasterListESA_Dec2018_20200427.csv'
# name of column to add
addcol = 'Range_Filename'  # Name of column to add
# location of GIS species spatial library files to add table either \CriticalHabitat or \Range
inlocation = r'D:\Species\Species Spatial Library\_CurrentFiles\Generalized files\Range'

addinfo_dict = {}
files_to_be_archived = []
index_pos_entid = 0  # entity id index position
# master species list
masterlist = r"D:\Species\Species Spatial Library\_CurrentFiles\CH_MasterListESA_Dec2018_20200427.csv"
outfile = outlocation +os.sep+ outfile_nm

def loop_all_species_gis(in_path):
    if in_path[-3:] == 'gdb':
        ent_list_final = []
        ingdb = in_path
        arcpy.env.workspace = ingdb
        fclist_final = arcpy.ListFeatureClasses()
        ent_list_final.extend([v.split("_")[1] for v in fclist_final])
        sp_dict = dict(zip(ent_list_final, fclist_final))
    else:
        fclist_final = []
        ent_list_final = []
        list_ws = os.listdir(in_path)
        print list_ws
        for v in list_ws:
            if v[-3:] == 'gdb':
                print v
                ingdb = in_path + os.sep + v
                arcpy.env.workspace = ingdb
                fclist = arcpy.ListFeatureClasses()
                fclist_final.extend(fclist)
                ent_list_final.extend([v.split("_")[1] for v in fclist])
            else:
                pass

        sp_dict = dict(zip(ent_list_final, fclist_final))
    return sp_dict


def add_column(row, filename_dict):
    global files_to_be_archived
    entid = str(row['EntityID'])
    try:
        file_nm = filename_dict[entid]
        return file_nm
    except:
        pass


def check_removed(df_child, df_parent):
    removed_species = []
    old_ent_list = df_child[df_child.columns.values.tolist()[index_pos_entid]].values.tolist()
    new_ent_list = df_parent[df_parent.columns.values.tolist()[index_pos_entid]].values.tolist()
    for entid in old_ent_list:
        if entid in new_ent_list:
            pass
        else:
            removed_species.append(entid)
    return removed_species


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

search_field = ['EntityID', 'FileName']
# loop_all_species_gis(inlocation, search_field, addinfo_dict)
addinfo_dict = loop_all_species_gis(inlocation)

master_list_df = pd.read_csv(masterlist)
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: x).astype(str)
[master_list_df.drop(v, axis=1, inplace=True) for v in master_list_df.columns.values.tolist() if v.startswith('Unnamed')]

master_list_df[addcol] = master_list_df.apply(lambda row: add_column(row, addinfo_dict), axis=1)
master_list_df.to_csv(outfile)



end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
