import arcpy
import os
import csv
import pandas as pd
import datetime

inlocation_species = r'L:\Workspace\ESA_Species\Range\HUC12\AllSpe\HUC12\GDB'
out_text_cross = inlocation_species +os.sep +'R_AllSpe_FWS_NMFS_ByHUC12_20170328_arr.txt'
out_text_cross_rename = inlocation_species +os.sep +'species_huc12_arr.txt'
out_text_huc12 = inlocation_species +os.sep +'R_AllSpe_FWS_NMFS_20170328_arr.csv'

out_csv_lists = inlocation_species +os.sep +'R_AllSpe_long_20170328_arr.csv'
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
                # strips list brackets, and single quotation marks around HUC12ID per Chucks requests
                out_val= str(out_info[k]).replace("'",'').replace(']','').replace('[','').replace(' ','').replace('u','')
                val = k,out_val

                writer.writerow(val)
    elif type(out_info) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for val in out_info:
                writer.writerow([val])


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

out_df = pd.DataFrame(index=(list(range(0, 40000))))
print out_df
for v in gdb_workspaces:
    print '\n{0}'.format(v)
    in_gdb = inlocation_species + os.sep + v
    arcpy.env.workspace = in_gdb
    fc_list = arcpy.ListFeatureClasses()
    count_fc = len(fc_list)
    running_count= 0
    for fc in fc_list:
        if running_count % 25== 0 or running_count == 0:
            print 'Working on {0} of {1}...'.format(running_count,count_fc)
        extract_id = fc.split("_")
        ent_id = extract_id[1]
        #print ent_id
        arr = arcpy.da.TableToNumPyArray(fc, ("HUC_12"))
        sp_df = pd.DataFrame(data=arr)
        sp_list =sp_df['HUC_12'].values.tolist()


        # with arcpy.da.SearchCursor(fc, "HUC_12") as cursor:
        #     sp_list = []
        #     for row in cursor:
        #         huc = str(row[0])
        #         if huc not in sp_list:
        #             sp_list.append(huc)
        #         if huc not in huc12_list:
        #             huc12_list.append(huc)
        #         else:
        #             continue

        spe_huc12= list(set(sp_list))
        # removes HUC12 without an ID from list
        spe_huc12= [x.replace("' ',)",'"",') if x ==("' ',)") else x for x in spe_huc12]
        species_dict[ent_id] = spe_huc12

        huc12_list.extend(spe_huc12)
        huc12_list =list(set(huc12_list))

        new_df = pd.DataFrame()
        se= pd.Series(spe_huc12)
        new_df [str(ent_id)]= se.values
        out_df=pd.concat([out_df,new_df],axis=1)
        running_count +=1


    print ("{0} and length of list is {1}".format(v, len(huc12_list)))

huc12_list =list(set(huc12_list))

create_outtable(species_dict,out_text_cross ,['EntityID', 'HUC12'])
create_outtable(species_dict,out_text_cross_rename ,['EntityID', 'HUC12'])
create_outtable(huc12_list,out_text_huc12  , ['HUC12'])
out_df.to_csv(out_csv_lists)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
