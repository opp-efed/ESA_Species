__author__ = 'JConno02'

import os
import unicodedata
import csv

from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

## TODO Tables do not follow standard HTML table an the species with populations are not loading correctly, must correct

# NOTE because of the above to do must check each individual table against final merge to see if anything dropped,
# specifically salmon and other species with populaations
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

verified_groups = ['Cetaceans', 'Pinnipeds', 'Sea Turtles', 'Other Marine Reptiles', 'Corals', 'Abalone', 'Fishes''Marine Plants' ]
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\Feb2017\NMFS'
out_csv = outpath + os.sep + 'Listed_'
url = "http://www.nmfs.noaa.gov/pr/species/esa/listed.htm"


def get_tables(htmldoc):
    soup = BeautifulSoup(htmldoc.content, 'html.parser')
    h2 = soup.find_all('h2')
    title = []
    for v in h2:
        t = v.getText()
        title.append(t)
    return soup.find_all('table'), title


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Download and parse website table
r = requests.get(url)
list_tables, title = get_tables(r)

counter = 0
for table in list_tables:

    table_header = title[counter]
    table_header = table_header.split(" ")
    for t in table_header:
        if t == '':
            pass
        else:
            table_header = unicodedata.normalize('NFKD', t)
            if t == 'Marine':
                pass
            else:
                break
    headers = [th.get_text() for th in table.find_all('th')]

    headers.append('Group')
    headers.append('Table_title')
    headers.append('Population')
    data = pd.DataFrame()
    tr_counts = (table.find_all('tr'))
    parse_counter = 0
    if table_header =='Plants':
        sp_data_list = [td.get_text() for td in table.find_all('td')]
        sp_data_list.insert(1, ' ')
        final_list = sp_data_list
        final_list.append(table_header)
        final_list.append(table_header)
        datalist = []
        for v in final_list:
            try:

                unicodedata.normalize('NFKD', v).encode('ascii', 'ignore')
                v.encode('utf-8').strip()
                v = v.decode('utf-8', 'ignore').encode("utf-8")
                v = v.replace('\n', '')

                datalist.append(v)
            except:
                v = v.replace('\n', '')
                datalist.append(v)
        break_name = datalist[0].split('(')
        common_name = (break_name[0])
        try:
            sci_name = str(break_name[1].replace(')', ''))
        except:
            sci_name = ''
        datalist.append(common_name)
        datalist.append(sci_name)
        row = (pd.DataFrame(data=(datalist))).T
        data = pd.concat([data, row], axis=0)


    else:
        while parse_counter < (len(tr_counts) - 1):

            current_tr = tr_counts[parse_counter]
            next_tr = tr_counts[parse_counter + 1]
            check_inner = [ul.get_text() for ul in next_tr.find_all('ul')]

            if len(check_inner) == 1:

                while len(check_inner) == 1:


                    sp_data_list = [td.get_text() for td in current_tr.find_all('td')]
                    populations_data_list = [td.get_text() for td in next_tr.find_all('td')]

                    populations_data_list.insert(0, sp_data_list[0])
                    final_list = populations_data_list
                    final_list.append(new_group)
                    final_list.append(table_header)
                    datalist = []
                    for v in final_list:
                        try:
                            unicodedata.normalize('NFKD', v).encode('ascii', 'ignore')
                            v.encode('utf-8').strip()
                            v = v.replace('\n', '')
                            v = v.decode('utf-8', 'ignore').encode("utf-8")
                            datalist.append(v)
                        except:
                            v = v.replace('\n', '')
                            datalist.append(v)
                    break_name = datalist[0].split('(')
                    common_name = (break_name[0]).decode('utf-8', 'ignore').encode("utf-8")

                    try:
                        sci_name = str(break_name[1].replace(')', ''))
                    except:
                        sci_name = ''
                    datalist.append(common_name)
                    datalist.append(sci_name)

                    row = (pd.DataFrame(data=(datalist))).T
                    data = pd.concat([data, row], axis=0)
                    parse_counter += 1

                    if parse_counter + 1 >= len(tr_counts):
                        break
                    else:
                        next_tr = tr_counts[parse_counter + 1]
                        check_inner = [ul.get_text() for ul in next_tr.find_all('ul')]
            else:
                sp_data_list = [td.get_text() for td in current_tr.find_all('td')]
                if len(sp_data_list) == 0:
                    pass
                elif len(sp_data_list) == 1:
                    grouptest = sp_data_list[0].strip(' ')
                    if grouptest not in verified_groups:
                        pass
                    else:
                        new_group = grouptest
                elif len(sp_data_list) > 1:
                    sp_data_list.insert(1, ' ')
                    final_list = sp_data_list
                    final_list.append(new_group)
                    final_list.append(table_header)
                    datalist = []
                    for v in final_list:
                        try:
                            unicodedata.normalize('NFKD', v).encode('ascii', 'ignore')
                            v.encode('utf-8').strip()
                            v = v.decode('utf-8', 'ignore').encode("utf-8")
                            v = v.replace('\n', '')

                            datalist.append(v)
                        except:
                            v = v.replace('\n', '')
                            datalist.append(v)
                    break_name = datalist[0].split('(')
                    common_name = (break_name[0])
                    try:
                        sci_name = str(break_name[1].replace(')', ''))
                    except:
                        sci_name = ''
                    datalist.append(common_name)
                    datalist.append(sci_name)
                    row = (pd.DataFrame(data=(datalist))).T
                    data = pd.concat([data, row], axis=0)

                parse_counter += 1

            # data.columns = headers

    try:
        out_csv_table = out_csv + str(table_header) + "_" + str(date) + '.csv'
    except:
        out_csv_table = out_csv + str('Marine_Mammals') + "_" + str(date) + '.csv'
        # print out_csv_table
    data.to_csv(out_csv_table, encoding='utf-8')
    counter += 1

# create_Finaltable(inter, (outpath + os.sep + 'NMFSb_' + str(counter) + '_' + str(date) + '.csv'))
##Fix ring seal- so that the pop is correct
### read in csv take the max values try to complete one row per pop
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
