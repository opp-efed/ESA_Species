import csv

infile = 'E:\NAS Pilot\Species\Bins\WOE_GroupAssign\WOE_speciesgroup_remove.csv'
outcsv = 'E:\NAS Pilot\Species\Bins\WOE_GroupAssign\WOE_speciesgroup_test.csv'
countDict ={}
ent_list= []
with open(infile)as countfile:
    reader = csv.DictReader(countfile)
    readerlist =csv.reader(countfile)
    for row in readerlist:
        entid = row[0]
        ent_list.append(entid)
    ent_set = set(ent_list)
        #print z
for z in ent_set:
    with open(infile )as countfile:
        readerlist =csv.reader(countfile)
        count = 0
        for row in readerlist:
            x=  row [0]
            if z== x:
                count = count +1
    countDict[z] = count
#print countDict

with open(infile )as orgfile:
    with open (outcsv, "wb")as outfile:
        reader =csv.reader(orgfile)
        writer =csv.writer(outfile,lineterminator='\n')

        all = []
        row =next(reader)
        row.append('Count')
        all.append(row)

        for row in reader:
            id= row[0]
            #print id
            count = countDict[id]
            row.append(count)
            all.append(row)
        writer.writerows(all)











