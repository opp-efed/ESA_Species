import arcpy
import os
import csv
import pandas as pd

inlocation_species = r'L:\Workspace\ESA_Species\Range\HUC12\AquWoes_FWS_NMFS\HUC12\GDB'
out_text_cross = r'L:\Workspace\ESA_Species\Range\HUC12\AquWoes_FWS_NMFS\HUC12\GDB\R_SpAquWoE_ByHUC12.txt'
out_text_huc12 = r'L:\Workspace\ESA_Species\Range\HUC12\AquWoes_FWS_NMFS\HUC12\GDB\R_HUC12_SpAquWoE.csv'
out_csv_lists = r'L:\Workspace\ESA_Species\Range\HUC12\AquWoes_FWS_NMFS\HUC12\GDB\R_SpAquWoE_long.csv'
workspaces = os.listdir(inlocation_species)
gdb_workspaces = [v for v in workspaces if v.endswith('.gdb')]
print gdb_workspaces
huc12_list = []
species_dict = {}


def create_outtable(out_info, csvname, header):
    # CHANGE added a function to export to the dictionaries and lists to csv to QA the intermediate steps and to have 
    # copies of the final tables
    if type(out_info) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for k, i in out_info.items():
                val = [k, out_info[k]]
                writer.writerow(val)
    elif type(out_info) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for val in out_info:
                writer.writerow([val])

out_df = pd.DataFrame(index=(list(range(0, 40000))))
print out_df
for v in gdb_workspaces:
    print v
    in_gdb = inlocation_species + os.sep + v
    arcpy.env.workspace = in_gdb
    fc_list = arcpy.ListFeatureClasses()
    print fc_list
    for fc in fc_list:
        extract_id = fc.split("_")
        ent_id = extract_id[1]
        #print ent_id
        with arcpy.da.SearchCursor(fc, "HUC_12") as cursor:
            sp_list = []
            for row in cursor:
                huc = str(row[0])
                if huc not in sp_list:
                    sp_list.append(huc)
                if huc not in huc12_list:
                    huc12_list.append(huc)
                else:
                    continue
            new_df = pd.DataFrame()
            spe_huc12= list(set(sp_list))
            species_dict[ent_id] = spe_huc12

            se= pd.Series(spe_huc12)
            new_df [str(ent_id)]= se.values
            out_df=pd.concat([out_df,new_df],axis=1)


    print ("{0} and length of list is {1}".format(v, len(huc12_list)))

create_outtable(species_dict,out_text_cross ,
                ['EntityID', 'HUC12'])
create_outtable(huc12_list,out_text_huc12  , ['HUC12'])
out_df.to_csv(out_csv_lists)


