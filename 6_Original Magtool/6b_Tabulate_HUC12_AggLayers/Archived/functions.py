import arcpy

def fcs_in_workspace(workspace):
  arcpy.env.workspace = workspace
  for fc in arcpy.ListFeatureClasses():
    yield(fc)
  for ws in arcpy.ListWorkspaces():
    for fc in fcs_in_workspace(ws):
        yield fc