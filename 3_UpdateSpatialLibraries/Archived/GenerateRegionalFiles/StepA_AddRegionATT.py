import datetime

import arcpy

inlocation = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\Composites\NL48_R_SpGroup_Composite_Web.gdb'
arcpy.env.workspace = inlocation
listfiles = arcpy.ListFeatureClasses()

start_script = datetime.datetime.now()

Region_cross = {'AK': 'AK',
                'AS': 'AS',
                'CNMI': 'CNMI',
                'GU': 'GU',
                'HI': 'HI',
                'Howland': 'Howland_Baker_Jarvis',
                'Johnston': 'Johnston',
                'L48': 'L48',
                'Lower48': 'L48',
                'PLower48': 'L48',
                'Laysan': 'Laysan',
                'Mona': 'Mona',
                'Necker': 'Necker',
                'Nihoa': 'Nihoa',
                'NorthwesternHI': 'NorthwesternHI',
                'PR': 'PR',
                'Palmyra': 'Palmyra Kingman',
                'VI': 'VI',
                'Wake': 'Wake'}
print listfiles
for v in listfiles:
    regions = v.split("_")
    regions = regions[0]
    regions = Region_cross[regions]
    print 'Adding {0} to file {1}'.format(regions, v)
    arcpy.AddField_management(v, "Region", "TEXT")
    with arcpy.da.UpdateCursor(v, "Region") as cursor:
        for row in cursor:
            row[0] = regions
            cursor.updateRow(row)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
