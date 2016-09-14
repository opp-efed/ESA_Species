import functions
import datetime
import os
import arcpy
# loop through folders using dict colunms final GDB, column, ID, acresID, total acres,fctype
# store acres in dicts by folder
# update percent overlap then update table

print 'start'
overlaplist = 'C:\WorkSpace\MasterOverlap\Range.gdb\MasterUse_20150903_R_20151208'  # arc_table
ch_overlaplist = 'C:\WorkSpace\MasterOverlap\CriticalHabitat.gdb\MasterUse_20150903_CH_20151208'

speField =['EntityID','COMNAME','SCINAME','FAMILY','STATUS_TEXT','POP_ABBREV','Des_CH','Critical_Habitat_','Migratory',
           'Migratory_','FileName','CH_Acres','CH_GIS','Group_','RAcres','Group','Both','L48_only','NL48_only']

# R_acres= table of acres by region
# CH_acres = table of ch by region


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

Rangelist_field = [f.name for f in arcpy.ListFields(overlaplist) if not f.required]
CHlist_field = [f.name for f in arcpy.ListFields(ch_overlaplist) if not f.required]

for field in Rangelist_field:
    if field in speField:
        continue
    else:
        print field
        with arcpy.da.UpdateCursor(overlaplist, [field]) as cursor:
            for row in cursor:
                row[0] = None
                cursor.updateRow(row)
                continue

for field in CHlist_field:
    if field in speField:
        continue
    else:
        print field
        with arcpy.da.UpdateCursor(ch_overlaplist, [field]) as cursor:
            for row in cursor:
                row[0] = None
                cursor.updateRow(row)
                continue


end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
