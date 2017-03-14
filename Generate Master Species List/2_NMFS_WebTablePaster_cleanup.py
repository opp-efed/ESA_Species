__author__ = 'JConno02'

import os
import unicodedata
import csv

from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import json
import urllib2

## TODO Tables do not follow standard HTML table an the species with populations are not loading correctly, must correct

# NOTE because of the above to do must check each individual table against final merge to see if anything dropped,
# specifically salmon and other species with populaations
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

groups = ['Cetaceans', 'Pinnipeds', 'Sea Turtles', 'Other Marine Reptiles', 'Corals', 'Abalone', 'Fishes']
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\Feb2017\NMFS\CanProposed'
out_csv = outpath + os.sep + 'CanProposed_'
# url = "http://www.nmfs.noaa.gov/pr/species/esa/listed.htm"
url = "http://www.nmfs.noaa.gov/pr/species/esa/candidate.htm#proposed"


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
    data = pd.DataFrame()
    table_header = title[counter]
    table_header = table_header.split(" ")

    for t in table_header:
        if t == '':
            pass
        else:
            table_header = t
            break
    headers = [th.get_text() for th in table.find_all('th')]

    headers.append('Group')
    headers.append('Table_title')

    tr_counts = (table.find_all('tr'))

    parse_counter = 0
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

    out_csv_table = out_csv + str(table_header) + "_" + str(date) + '.csv'

    # print out_csv_table
    data.to_csv(out_csv_table, encoding='utf-8')
    counter += 1
    #
    # if counter ==0:
    #
    #
    #
    #
    #
    #     for tr in table.find_all('tr'):
    #         data_list = [td.get_text() for td in tr.find_all('td')]
    #
    #         if len(data_list) == 0:
    #             pass
    #         elif len(data_list) ==1:
    #             new_group = data_list[0]
    #         else:
    #             data_list.append(new_group)
    #             data_list.append(table_header)
    #             row = (pd.DataFrame(data=(data_list))).T
    #
    #             data = pd.concat([data, row ], axis = 0)
    #
    #     data.columns= headers
    #     #print data
    #     out_csv_table = out_csv+str(table_header)+"_"+str(date)+'.csv'
    #     #print out_csv_table
    #     data.to_csv(out_csv_table ,encoding='utf-8')
    #     counter += 1
    # else:
    #     table_header = title[counter]
    #     table_header =table_header.split(" ")
    #
    #     for t in table_header:
    #         if t =='':
    #             pass
    #         else:
    #             table_header = t
    #             break
    #
    #     print table_header
    #     headers = [th.get_text() for th in table.find_all('th')]
    #
    #     headers.append('Group')
    #     headers.append('Table_title')
    #     data = pd.DataFrame()
    #     for tr in table.find_all('tr'):
    #         for td in tr.find_all('td'):
    #             data_list = [ul.get_text() for ul in td.find_all('ul')]
    #             print data_list
    #     #     data_list = [td.get_text() for td in tr.find_all('td')]
    #     #
    #     #     if len(data_list) == 0:
    #     #         pass
    #     #     elif len(data_list) ==1:
    #     #         new_group = data_list[0]
    #     else:
    #         data_list.append(new_group)
    #         data_list.append(table_header)
    #         row = (pd.DataFrame(data=(data_list))).T
    #
    #         data = pd.concat([data, row ], axis = 0)
    #
    # data.columns= headers
    # #print data
    # out_csv_table = out_csv+str(table_header)+"_"+str(date)+'.csv'
    # #print out_csv_table
    # data.to_csv(out_csv_table ,encoding='utf-8')
    # counter += 1
#
# # create_Finaltable(inter, (outpath + os.sep + 'NMFSb_' + str(counter) + '_' + str(date) + '.csv'))
# ##Fix ring seal- so that the pop is correct
# ### read in csv take the max values try to complete one row per pop
# end = datetime.datetime.now()
# print "End Time: " + end.ctime()
# elapsed = end - start_time
# print "Elapsed  Time: " + str(elapsed)

# rows = soup.find_all("h2")
# print rows
# headers = {}
#
# thead = soup.find_all("table")
# print thead
#
# for i in range(len(thead)):
#     headers[i] = thead[i].text.strip().lower()
#
# data = []
#
# for row in rows:
#     cells = row.find_all("td")
#
#     item = {}
#
#     for index in headers:
#         item[headers[index]] = cells[index].text
#
#     data.append(item)
#
# print(json.dumps(data, indent=4))



# table_data = [[cell.text for cell in row("td")]
#                          for row in BeautifulSoup(r.content,'html.parser')("tr")]
# print table_data

#
# print soup
#
# # break out by h2 tag
# h2 = soup.find_all('h2')
# title = []
# for v in h2:
#     t = v.getText()
#     title.append(t)
#
# # find all the tables
# tables = soup.find_all('table')
#
# counter = 0
# final = []
# inter = []
#
# # Parse the tables into a list of info for each species
# for table in tables:
#     t = unicodedata.normalize("NFKD", title[counter])
#     # subgroup = 'None'
#     # print table.prettify()
#     outtable = makelist(table)
#     print outtable
#     header = t
#     create_outtable(outtable, (outpath + os.sep + 'NMFS_' + str(counter) + '_' + str(date) + '.csv'), header)
#     current_species = []
#
#     # loads species into into a list for each species
#     pop = False
#     for v in outtable:
#         # print len(v)
#         current_species = []
#         # print current_species
#
#         if len(v) == 2:
#             subgroup = None
#             breaklist = v[0].split('(')
#             if len(breaklist) == 1:
#                 subgroup = " ".join(v[0].split())
#             else:
#                 pop = True
#                 commonname = unicodedata.normalize("NFKD", breaklist[0])
#                 sciname = unicodedata.normalize("NFKD", breaklist[(len(breaklist) - 1)].replace(')', ''))
#         elif len(v) == 6:
#             spe_infoList = v[0].split('(')
#             print len(spe_infoList)
#             if len(spe_infoList) == 1:
#                 current_species.append(commonname)
#                 current_species.append(sciname)
#                 for i in v:
#                     current_species.append(unicodedata.normalize("NFKD", i))
#                 print pop
#                 continue
#
#             elif len(spe_infoList) >= 2:
#                 com = unicodedata.normalize("NFKD", spe_infoList[0])
#                 sci = unicodedata.normalize("NFKD", spe_infoList[(len(spe_infoList) - 1)].replace(')', ''))
#                 current_species.append(com)
#                 current_species.append(sci)
#                 for i in v:
#                     current_species.append(unicodedata.normalize("NFKD", i))
#         else:
#             pass
#         try:
#             print commonname
#             print sciname
#         except:
#             pass
#         print current_species
#         if len(current_species) != 0:
#             inter.append(current_species)
#
#     counter += 1
# # print inter
# outDF_Full = pd.DataFrame(inter)
# fulltable = (outpath + os.sep + 'NMFSb_' + str(counter) + '_' + str(date) + '.csv')
