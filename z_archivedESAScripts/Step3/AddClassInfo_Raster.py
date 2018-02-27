import arcpy
import datetime
import os

inlocation = r'J:\Workspace\Step3_Proposal\Yearly_CDL\CDL_reclass_new.gdb'
fieldname = 'CDL_Gen_Class'
KeyField = 'Value'
start_script = datetime.datetime.now()

print 'Script start at {0}'.format(start_script)
#What should values 0 and 73 be considered
EPA_CDL_RecodeKey = {0: 'Background',
                     10: 'Corn',
                     14: 'Corn/soybeans',
                     15: 'Corn/wheat',
                     18: 'Corn/grains',
                     20: 'Cotton',
                     25: 'Cotton/wheat',
                     26: 'Cotton/vegetables',
                     30: 'Rice',
                     40: 'Soybeans',
                     42: 'Soybeans/cotton',
                     45: 'Soybeans/wheat',
                     48: 'Soybeans/grains',
                     50: 'Wheat',
                     56: 'Wheat/vegetables',
                     58: 'Wheat/grains',
                     60: 'Vegetables and ground fruit',
                     61: '(ground fruit)',
                     68: 'Vegetables/grains',
                     70: 'Orchards and grapes',
                     73: 'Unknown',
                     75: 'Other trees',
                     80: 'Other grains',
                     90: 'Other row crops',
                     100: 'Other crops',
                     110: 'Pasture/hay/forage',
                     121: 'Developed - open',
                     122: 'Developed - low',
                     123: 'Developed - med',
                     124: 'Developed - high',
                     140: 'Forest',
                     160: 'Shrubland',
                     180: 'Water',
                     190: 'Wetlands - woods',
                     195: 'Wetlands - herbaceous',
                     200: 'Miscellaneous land'}

path, tail = os.path.split(inlocation)

# Check in location is a workspace or single raster and then generate a list of rasters to process
if str(tail)[-3:] == 'gdb' or ((tail)[-4:])[:1] == '.':
    arcpy.env.workspace = inlocation
    raster_list = arcpy.ListRasters()
else:
    raster_list = [inlocation]

for v in raster_list:
    arcpy.MakeRasterLayer_management(v, "lyr")
    arcpy.AddField_management("lyr", fieldname, 'TEXT')
    with arcpy.da.UpdateCursor("lyr", [KeyField, fieldname]) as cursor:
        for row in cursor:
            key = row[0]
            row[1] = str(EPA_CDL_RecodeKey[key])
            cursor.updateRow(row)
    print '     Loop Completed in {0} for {1}'.format(((datetime.datetime.now()) - start_script), v)
print 'Script Completed {0}'.format((datetime.datetime.now()) - start_script)
