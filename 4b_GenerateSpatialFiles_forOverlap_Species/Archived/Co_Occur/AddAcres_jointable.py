import arcpy
ingdb ='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Composites\GDB\NL48_ProjectedSpGroupComposites.gdb'
intable = 'C:\Users\Admin\Documents\Jen\Workspace\MasterLists\CSV\Masterlist_20151120.dbf'
region_c='PR'
field="PR_R"

def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

for fc in fcs_in_workspace(ingdb):
    print fc
    region=fc.split("_")
    region=str(region[0])

    if region ==region_c:
        acres_dict ={}
        entlist =[]
        with arcpy.da.SearchCursor (fc,["EntityID","Acres"]) as cursor:
            for row in cursor:
                ent =row[0]
                acres_dict[ent]=row[1]
                entlist.append(ent)
            del row, cursor
        with arcpy.da.UpdateCursor(intable,["EntityID",field]) as cursor:
            for row in cursor:
                ent= row[0]
                if ent in entlist:
                    acres = acres_dict[ent]
                    row[1]=acres
                    cursor.updateRow(row)
    else:
        continue

