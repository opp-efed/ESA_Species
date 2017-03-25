import datetime
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

# NOTES FOR UPDATES- J.Connolly 3/24/2017
# ## when new species have been added, species with multiple pops with sometimes have merge cell eg false killer whale
# ## may have split cells with some information that applies to all pops like humpback
# ## may have split cells without info like ring seal, loggerhead and Pacific eulachon
# # match the new species to exceptions already coded in here

# NOTE array length error: If species lists are not the same length print out each list to see which one is shorter
# Description: J. Connolly 3/24/2017
# Loop through each tables and extract species information into the standard lists set in static variables
# List will be combined into a single data frame then exported
# Logic for parse the tables is complicated due to merged cells. There are five columns of information but will
# sometimes include merged cell.  A merged cell may mean the start of a new species group, a species with multiple
# populations with varying support data.

# Species info is extracted from columns and append to appropriate lists. All values are
# .encode ('ascii', 'replace').replace('?', ' ')) to remove excess html characters

# ComName, SciName, Pop Name column 1 (info is split into name_list then extracted by index position,
# Year list column 2(info is split into list for species that had status updated since original listing.
# Only the year of the most recent status is extracted)
# Status column 3 (info is split into list for species that had status updated since original listing.
# Only the status is extracted)
# CritHab column 4 (info is split to removed excess html characters)
# Recovery plan column 5 (info is split to removed excess html characters)

# #################### VARIABLES
# #### user input variables
outlocation = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017\NMFS' # path final tables
# Species groups used by NMFS
groups = ['Cetaceans', 'Pinnipeds', 'Sea Turtles', 'Other Marine Reptiles', 'Corals', 'Abalone', 'Fishes',
          'Sea Turtles', 'Other Marine Reptiles']
# NMFS website with the tables of listed species
url = "http://www.nmfs.noaa.gov/pr/species/esa/listed.htm"
#foreign species per NMFS
removed_perNMFS =['Pristis pristis formerly P. perotteti, P. pristis, and P. microdon']
# statuses that will be included when final table is filtered
# Note: NMFS experimental populations do not fall under section 7, see notes from T Hooper
section_7_status = ['E', 'T']
# ####static default variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# empty lists to store data; and default boolean assumptions
species_com = []
species_sci = []
year = []
status = []
pop = []
crithab = []
recovery = []
group = []
multi_pop = False
# Functions
def get_tables(htmldoc):  # h2 is the tag for the table header, this will parse soup to get the html table header,
    # then extract the text
    soup = BeautifulSoup(htmldoc.content, 'html.parser')
    h2 = soup.find_all('h2')
    title_list = []
    for v in h2:
        t = v.getText().lstrip()
        title_list.append(t)
    return soup.find_all('table'), title_list

# ######################################################################################################################
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1:Download and parse out website tables
r = requests.get(url)
list_tables, title = get_tables(r)
counter = 0  # counter to extract the current table header
# Step 2: For each table parse out species information in each table column and load it into the appropriate lists
for table in list_tables:
    table_name = title[counter]
    print table_name
    # set species group for tables that does not indicated the species group within the original table
    if table_name == 'Fish (Marine & Anadromous) (66 listed "species")':
        current_group = 'Fishes'
    if table_name == 'Marine Plants (1 listed "species")':
        current_group = 'Plants'

    # each row in the table will start ,<tr> tag pair, this will skip the first and move to the second
    for row in table.find_all('tr')[1:]:
        # Create a variable of all the <td> tag pairs in each <tr> tag pair,this is the col information for the row
        col = row.find_all('td')
        if len(col) == 1:  # If there is only one value the row is merged across all columns; this indicated the start
            # of a new species group or the continuation of a species that has multiple populations
            column_1 = col[0].get_text().lstrip()
            if column_1 in groups:
                current_group = column_1
                multi_pop = False

            elif column_1 not in groups:
                column_1 = col[0].get_text()
                name_list = column_1.split("\n")

                if len(name_list) >= 2:
                    comm_name = name_list[0].encode('ascii', 'replace').replace('?', ' ')
                    sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                    sci = sci.replace(')', '')
                    multi_pop = True
                else:
                    pass

        if len(col) == 5:  # if the length <td> tags is equal to the number of columns there is information in each
            # column.  This can be a complete species or a single population of a speices
            column_1 = col[0].get_text()
            name_list = column_1.split("\n")  # list of information found in column 1, common name, sci name, pop name

            # exceptions for species with multiple populations that do not start with a merge cell across all columns
            # before the populations are listed
            if name_list[0] == 'whale, humpback (5 DPSs)':
                comm_name = name_list[0].encode('ascii', 'replace').replace('?', ' ')
                sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                sci = sci.replace(')', '')
                multi_pop = True

            elif name_list[0] == 'whale, North Atlantic right':
                # original listing triggered the multiple populations however is not included in final list
                comm_name = name_list[0].encode('ascii', 'replace').replace('?', ' ')
                sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                sci = sci.replace(')', '')
                multi_pop = True

            elif name_list[0] == 'whale, North Pacific right':
                # original listing triggered the multiple populations however is not included in final list
                comm_name = name_list[0].encode('ascii', 'replace').replace('?', ' ')
                sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                sci = sci.replace(')', '')
                multi_pop = True

            elif name_list[1] == '(Caretta caretta)':
                comm_name = (name_list[0]).encode('ascii', 'replace').replace('?', ' ')
                sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                sci = sci.replace(')', '')
                multi_pop = True

            elif name_list[1] == '(Thaleichthys pacificus)':
                comm_name = (name_list[0]).encode('ascii', 'replace').replace('?', ' ')
                sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                sci = sci.replace(')', '')
                multi_pop = True

            elif len(name_list) == 2:
                if name_list[1].startswith('(Phoca hispida'):
                    multi_pop = True
                else:
                    multi_pop = False


            if multi_pop:  # If a species has multiple population boolean multi_pop is set to true so that the
                # common name and sci name  is repeated for each of the populations

                # Exceptions for species with multiple populations that do not start with a merge cell across
                # all columns. Columns 2-5 will be gridded for these species in the original URL table
                if sci == 'Caretta caretta':
                    species_sci.append(sci)
                    species_com.append(comm_name)
                    pop_name = (name_list[2].encode('ascii', 'replace').replace('?', ' '))
                    pop_name =pop_name.replace(' DPS','')
                    pop.append(pop_name)

                elif len(name_list) == 2:
                    if name_list[1].startswith('(Phoca hispida'):
                        comm_name = name_list[0].encode('ascii', 'replace').replace('?', ' ')
                        sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                        sci = sci.replace(')', '')
                        species_sci.append(sci)
                        species_com.append(comm_name)
                        pop.append('None')
                    elif name_list[1].startswith('(Thaleichthys pacificus'):
                        comm_name = name_list[0].encode('ascii', 'replace').replace('?', ' ')
                        sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                        sci = sci.replace(')', '')
                        species_sci.append(sci)
                        species_com.append(comm_name)
                        pop.append('None')
                else:
                    species_sci.append(sci)
                    species_com.append(comm_name)
                    pop_name = (name_list[2].encode('ascii', 'replace').replace('?', ' '))
                    pop_name =pop_name.replace(' DPS','')
                    if pop_name == ' ':
                        pop.append('None')
                    else:
                        pop.append(pop_name)

                group.append(current_group)
                column_3 = col[1].get_text().encode('ascii', 'replace').replace('?', ' ')

                if name_list[0] == 'whale, North Atlantic right':
                    column_3 = '2008'
                    year.append(column_3)

                elif name_list[0] == 'whale, North Pacific right':
                    column_3 = '2008'
                    year.append(column_3)

                else:
                    year_list = column_3.split('\n')
                    year.append(year_list[0].replace('*', '').replace('+', ''))

                column_4 = col[2].get_text().encode('ascii', 'replace').replace('?', ' ')
                status_list = column_4.split('\n')
                status.append(status_list[0])

                column_5 = col[3].get_text().encode('ascii', 'replace').replace('?', ' ')
                crithab_list = column_5.split('\n')
                crithab.append(crithab_list[0])

                column_6 = col[4].get_text().encode('ascii', 'replace').replace('?', ' ')
                if comm_name == 'whale, humpback (5 DPSs)':
                    recovery.append('final')
                else:
                    recovery_list = column_6.split('\n')
                    recovery.append(recovery_list[0].replace('*', '').replace('+', ''))
            else:  # species with one population represented on a single row
                column_1 = col[0].get_text().encode('ascii', 'replace').replace('?', ' ')
                name_list = column_1.split("\n")
                comm_name = name_list[0]

                sci = (name_list[1].encode('ascii', 'replace').replace('?', ' ').replace('(', ''))
                sci = sci = sci.replace(')', '')

                species_sci.append(sci)
                species_com.append(comm_name)
                pop.append('None')
                group.append(current_group)

                column_3 = col[1].get_text().encode('ascii', 'replace').replace('?', ' ')
                year_list = column_3.split('\n')
                year.append(year_list[0].replace('*', '').replace('+', ''))

                column_4 = col[2].get_text().encode('ascii', 'replace').replace('?', ' ')
                status_list = column_4.split('\n')
                status.append(status_list[0])

                column_5 = col[3].get_text().encode('ascii', 'replace').replace('?', ' ')
                crithab_list = column_5.split("\n")
                crithab.append(crithab_list[0])

                column_6 = col[4].get_text().encode('ascii', 'replace').replace('?', ' ')
                recovery_list = column_6.split("\n")
                recovery.append(recovery_list[0].replace('*', '').replace('+', ''))

    counter += 1
# # NOTE array length error:
# # print len(species_sci), len(species_com),len(status),len(year),len(crithab),len(recovery),len(pop),len(group)
# Step 3: Loads species lists into a dictionary and then a data frame
columns = {'Invname': species_com, 'Scientific Name': species_sci, 'Population': pop, 'Status': status,
           'Year Listed': year, 'Critical Habitat': crithab, 'Recovery Plan': recovery, 'Group_B': group}
df = pd.DataFrame(columns,
                  columns=['Invname', 'Scientific Name', 'Population', 'Year Listed', 'Status', 'Critical Habitat',
                           'Recovery Plan', 'Group_B'])
# Step 4: Filters data frame to include only the statuses of concern for section 7
remove_foreign = df.loc[df['Status'].isin(section_7_status) == True]
remove_foreign = remove_foreign [remove_foreign ['Scientific Name'].isin(removed_perNMFS) == False]

# Step 5: Exports both the full list from the website and filtered list to csvs
df.to_csv(outlocation + os.sep + 'FullWebsite_NMFS_Listed' + date + '.csv', encoding='utf-8')
remove_foreign.to_csv(outlocation + os.sep + 'FilteredWebsite_NMFS_Listed' + date + '.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
