import arcpy
import os
import csv

inlocation_species = 'L:\Workspace\ESA_Species\Range\HUC12\AquWoes_FWS_NMFS\HUC12\GDB'
workspaces = os.listdir(inlocation_species)
gdb_workspaces = [v for v in workspaces if v.endswith('.gdb')]
print gdb_workspaces
huc12_list = []
species_dict ={}
def create_outtable(outInfo, csvname, header):
    ## CHANGE added a function to export to the dictionaries and lists to csv to QA the intermediate steps and to have copies of the final tables
    if type(outInfo) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for k, v in outInfo.items():
                val = []
                val.append(k)
                val.append(outInfo[k])
                writer.writerow(val)
    elif type(outInfo) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for val in outInfo:
                writer.writerow([val])

for v in gdb_workspaces:
    print v
    in_gdb = inlocation_species+os.sep+v
    arcpy.env.workspace = in_gdb
    fclist =arcpy.ListFeatureClasses()
    print fclist
    for fc in fclist:
        extract_id = fc.split(("_"))
        entid= extract_id[1]
        print entid
        with arcpy.da.SearchCursor (fc, "HUC_12") as cursor:
            sp_list= []
            for row in cursor:
                huc = str(row[0])

                if huc not in sp_list:
                    sp_list.append(huc)
                if huc not in huc12_list:
                    huc12_list.append(huc)
                else:
                    continue

            species_dict[entid]= set(sp_list)

    print ("{0} and lenght of list is {1}".format(v, len(huc12_list)))


create_outtable(species_dict,'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\Species_ByHUC12_All.txt',['EntityID','HUC12'])
create_outtable(huc12_list,'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\HUC12_All.csv',['HUC12'])

