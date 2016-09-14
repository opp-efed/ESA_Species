import functions
import datetime
import os
import arcpy
# loop through folders using dict colunms final GDB, column, ID, acresID, total acres,fctype
# store acres in dicts by folder
# update percent overlap then update table

print 'start'

ch_overlaplist  = 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Collapsed_CriticalHabitat.gdb\MasterUse_20150903_CH_201512010'
overlaplist= 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Collapsed_Range.gdb\MasterUse_20150903_R_201512010'

usegroups = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\Collapse_groups.csv'
# R_acres= table of acres by region
# CH_acres = table of ch by region

#######Corect tab from archive
start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

skiplist = []
#, 'Rangeland', 'Rice',
# 'Soybeans', 'VI_Ag', 'VegetablesGroundFruit', 'VegetablesGroundFruit_DiazBuffer', 'Wheat', 'WildLand_L48', 'XmasTrees']
#'AK_Ag', 'AS_Ag', 'CNMI_Ag', 'Corn', 'Cotton', 'CullPiles', 'Developed', 'Diazinon_ActionArea',
            # 'FWSApproved_inclusiveonly', 'FedLands_L48', 'GU_Ag', 'Golfcourses', 'HI_Ag', 'IndianLand_L48',
             #'ManagedForests', 'Nurseries' ,'Nurseries_DiazBuffer', 'OSD', 'OrchardsVineyards',
             #'OrchardsVineyards_DiazBuffer', 'OtherCrops', 'OtherGrains', 'OtherRowCrops', 'PR_Ag', 'Pasture',
             #'PineSeedOrchards', 'ROW',
textField = ['FileName', 'CH_GIS','Group_']

complete_fieldlist = ['Corn','Cotton','OrchardsVineyards','OtherCrops',
'OtherGrains','OtherRowCrops','Pasture','Rice','Soybeans','VegetablesGroundFruit','Wheat',
'Developed','ManagedForests','Nurseries',
'OSD','PineSeedOrchards','ROW','XmasTrees','Golfcourses','Rangeland','Mosquito Control','Wide Area Use','FileName', 'CH_GIS','Group_','AK_Ag','AS_Ag',
'HI_Ag','CNMI_Ag','GU_Ag','PR_Ag','VI_Ag','Nurseries_DiazBuffer','VegetablesGroundFruit_DiazBuffer','OrchardsVineyards_DiazBuffer',
'Diazinon_ActionArea','FedLands_L48',
'FWSApproved_inclusiveonly','IndianLand_L48','WildLand_L48','CullPiles','AK_Ag','AK_CattleE','AK_Develop','AK_Managed',
'AK_Nurseri','AK_OSD','AK_ROW','AS_Ag','AS_Develop','AS_OSD',
'AS_Rangela','AS_ROW','CNMI_Ag','CNMI_Devel','CNMI_Manag','CNMI_OSD','CNMI_Range',
'CNMI_ROW','CONUS_Catt','CONUS_Corn','CONUS_Cott','CONUS_Cull','CONUS_Deve',
'CONUS_Mana','CONUS_Nu_1','CONUS_Nurs','CONUS_Or_1','CONUS_Orch','CONUS_OSD','CONUS_Ot_1',
'CONUS_Ot_2','CONUS_Othe','CONUS_Past','CONUS_Pine','CONUS_Rice',
'CONUS_ROW','CONUS_Soyb','CONUS_Ve_1','CONUS_Vege','CONUS_Whea','CONUS_Xmas','Diazinon_A',
'FedLands_L','FWSApprove','Golfcourse','GU_Develop',
'GU_Managed','GU_OSD','GU_Rangela','GU_ROW','HI_Develop','HI_Managed','HI_Nurseri',
'HI_Orchard','HI_OSD','HI_OtherCr','HI_OtherGr','HI_Pasture',
'HI_Rangela','HI_ROW','HI_Vegetab','IndianLand','PR_Develop','PR_Managed','PR_Nurseri',
'PR_Orchard','PR_OSD','PR_OtherCr','PR_OtherGr','PR_Rangela',
'PR_ROW','PR_Vegetab','VI_Develop','VI_Managed','VI_Nurseri','VI_OSD','VI_Rangela',
'VI_ROW','WildLand_L']


Rangelist_field = [f.name for f in arcpy.ListFields(overlaplist) if not f.required]
CHlist_field = [f.name for f in arcpy.ListFields(ch_overlaplist) if not f.required]
for field in complete_fieldlist:
    if field not in Rangelist_field:
        try:
            if field in textField:
                arcpy.AddField_management(overlaplist, field, "TEXT")
            else:
                arcpy.AddField_management(overlaplist, field, "DOUBLE", "", 0)

        except:
            continue
    else:
                continue



for field in complete_fieldlist:
    if field not in CHlist_field:
        try:
            if field in textField:
                arcpy.AddField_management(overlaplist, field, "TEXT")
            else:
                arcpy.AddField_management(overlaplist, field, "DOUBLE", "", 0)

        except:
            continue
    else:
                continue
print "finished adding"


collaped_group_list = []
with open(usegroups, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        org_group = str(line[2])
        collaped_group = str(line[1])

        collaped_group_list.append(str(collaped_group))

inputFile.close()
set_collapse_group_list = sorted(set(collaped_group_list))
print set_collapse_group_list

for field in set_collapse_group_list:
    if field in skiplist:
        continue
    current_collapsed = []
    print field
    with open(usegroups, 'rU') as inputFile:
        for line in inputFile:
            line = line.split(',')
            org_group = str(line[2])
            org_group = org_group.strip("\n")
            collaped_group = str(line[1])
            if collaped_group == field:
                current_collapsed.append(org_group)

            else:
                continue
    current_collapsed.append("EntityID")
    print current_collapsed
    if len(current_collapsed) == 2:
        continue
    else:
        cursor = arcpy.SearchCursor(overlaplist)

        for row in cursor:
            total = 0
            ent = row.getValue("EntityID")
            print "EntityID is {0}".format(ent)
            for f in current_collapsed:
                if f == "EntityID":
                    continue
                else:
                    print f
                    current = (row.getValue(str(f)))
                    print(current)
                    if current <0:
                        continue
                    elif current is None:
                        continue

                    else:
                        current = float(current)
                        total = total + current


            print "Total is {0}".format(total)
                        # print total

            with arcpy.da.UpdateCursor(overlaplist, ["EntityID", field]) as update:
                    # print "Updating range species {0} and field {1}".format(ent, field)
                    for line in update:
                        entid = line[0]
                        if entid == ent:
                            line[1] = total
                            update.updateRow(line)
                        else:
                            continue
                        del line, update
        del row, cursor
        cursor = arcpy.SearchCursor(ch_overlaplist)

        for row in cursor:
            total = 0
            ent = row.getValue("EntityID")
            print "EntityID is {0}".format(ent)
            for f in current_collapsed:
                if f == "EntityID":
                    continue
                else:
                    print f
                    current = (row.getValue(str(f)))
                    print current
                    if current <0:
                        continue
                    elif current is None:
                        continue

                    else:
                        current = float(current)
                        total = total + current
                    print "Total is {0}".format(total)

                with arcpy.da.UpdateCursor(ch_overlaplist, ["EntityID", field]) as update:
                    # print "Updating range species {0} and field {1}".format(ent, field)
                    for line in update:
                        entid = line[0]
                        if entid == ent:
                            line[1] = total
                            update.updateRow(line)
                        else:
                            continue
                    del line, update
        del row, cursor

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
