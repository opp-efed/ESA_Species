import arcpy
import os

CH_intable = 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\CriticalHabitat.gdb\MasterUse_20150903_CH_20151209'
R_intable = 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Range.gdb\MasterUse_20150903_R_20151209'

acres_R ='C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Acres.gdb\Acres_20151207_Range'
acres_CH ='C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Acres.gdb\Acres_20151207_CriticalHabitat'

def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

ch_acres = {}
r_acres={}
with arcpy.da.SearchCursor(acres_R, ["EntityID", "Full_R"]) as cursor:
    for row in cursor:
        ent = str(row[0])
        range = str(row[1])
        r_acres[ent]= range

with arcpy.da.SearchCursor(acres_CH, ["EntityID",'Full_CH']) as cursor:
    for row in cursor:
        ent = str(row[0])
        ch=str(row[1])
        ch_acres[ent] = ch


with arcpy.da.UpdateCursor(R_intable, ["EntityID", "RAcres","FileName"]) as cursor:
    for row in cursor:
        ent = str(row[0])
        print ent
        try:
            acres = r_acres[ent]
            row[1] = acres
            cursor.updateRow(row)
        except:
            continue
with arcpy.da.UpdateCursor(CH_intable, ["EntityID", "CH_Acres", "FileName"]) as cursor:
    for row in cursor:
        ent = str(row[0])
        print ent
        try:
            acres = ch_acres[ent]
            row[1] = acres
            cursor.updateRow(row)
        except:
            continue

