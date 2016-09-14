import csv
import os
import datetime

Assignmentdict = "C:\Users\JConno02\Documents\Projects\ESA\WOE\inputTables\AssignmentDicts\Assign_20151105.csv"
binlong ="C:\\Users\\JConno02\\Documents\\Projects\\ESA\\Bins\\ExportDataBase\\20151105\\Aquatic_wReassign_20151105_long.csv"
csvlocation = "C:\\Users\\JConno02\\Documents\\Projects\\ESA\\Bins\\ExportDataBase\\20151105"
def createdicts(csvfile):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname


start = datetime.datetime.now()
print "Script start at {0}".format(start)

if not os.path.exists(csvlocation):
    os.mkdir(csvlocation)
    print "created directory {0}".format(csvlocation)


Assignment_dict = createdicts(Assignmentdict)
final =[]
with open (binlong,'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line= line.split(',')
        print line
        value = line [19]
        value=value.strip('\n')
        print value
        assign = Assignment_dict[value]
        line.remove(line[19])
        line.append(assign)
        final.append(line)
inputFile.close()

f = open(csvlocation + os.sep + 'AquaticOnly_20151105_long_assigned.csv', "wb")
writer = csv.writer(f, delimiter=' ', quotechar=',', quoting=csv.QUOTE_MINIMAL)
writer.writerow([header])
for val in final:
    writer.writerow([val])
