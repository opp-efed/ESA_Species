import os

from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import sys

# Pull structure from http://chrisalbon.com/python/beautiful_soup_scrape_table.html
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

groups = ['Cetaceans', 'Pinnipeds', 'Sea Turtles', 'Other Marine Reptiles', 'Corals', 'Abalone', 'Fishes',
          'MarineMammals', 'MarineInvertebrates']

url = "http://www.nmfs.noaa.gov/pr/species/esa/candidate.htm#proposed"
outlocation = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\September2017'

# Can and Proposed foreign species per NMFS
removed_perNMFS =['Cephalorhynchus hectori maui','Cephalorhynchus hectori hectori','Sousa chinensis taiwanensis',
                  'Mustelus schmitti','Squatina guggenheim','Amblyraja radiata','Rhinobatos horkelii',
                  'Squatina argentina','Isogomphodon oxyrhynchus','Mustelus fasciatus']

def check_for_exceptions():
    possAnswer = ['Yes', 'No']
    ask_preQ = True
    while ask_preQ:
        default_answers = raw_input('Have you check for unstructured species? {0}: '.format(possAnswer))

        if default_answers  not in possAnswer:
            print 'This is not a valid answer: remove quotes and spaces'
        else:
            break
    if default_answers=='Yes':
                pass
    else:
        print '\nCheck for unstructured species such as bullets, or merged cells that do not follow structure-see notes'
        sys.exit()



def get_tables(htmldoc):
    soup = BeautifulSoup(htmldoc.content, 'html.parser')
    h2 = soup.find_all('h2')
    title = []
    for v in h2:
        t = v.getText()
        title.append(t)
    return soup.find_all('table'), title


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

check_for_exceptions()

createdirectory(outlocation+os.sep+'NMFS')
outlocation = outlocation+os.sep+'NMFS'
# Download and parse website table
r = requests.get(url)

list_tables, title = get_tables(r)

species_com = []
species_sci = []
year = []
status = []
group = []
pop = []
counter = 0
for table in list_tables:
    for row in table.find_all('tr')[1:]:
        pop_name = 'None'
        # Create a variable of all the <td> tag pairs in each <tr> tag pair,
        col = row.find_all('td')
        print col

        if len(col) == 1:
            column_1 = col[0].get_text().replace(' ', '')
            if column_1 == 'MarineInvertebrates':
                current_group = 'Marine Invertebrates'
            elif column_1 == 'MarineMammals':
                current_group = 'Marine Mammals'
            elif column_1 in groups:
                current_group = column_1
            else:
                pass
        elif len(col) == 2:
            column_1 = col[0].get_text()
            name_list = (column_1).split("\n")

            if name_list[0] == '':
                if name_list[1] == '':
                    counter_a = 2
                    check_end = name_list[counter_a]
                    if com_name == "dolphin, Hector's (2 subspecies)":

                        sci_name = name_list[3]
                        pop_name = name_list[2]
                        sci = (sci_name.replace('(', ''))
                        sci = sci.replace(')', '')

                        species_com.append(com_name)
                        species_sci.append(sci)
                        pop.append(pop_name)
                        group.append(current_group)

                        # and append it to last_name variable

                        year.append(str(column_2))

                        # Create a variable of the string inside 3rd <td> tag pair,
                        column_3 = col[1].get_text()
                        column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')
                        check_fr = column_3.split(' ')

                        if 'FR' in check_fr:
                            current_status = 'candidate'
                        else:
                            current_status = column_3
                        status.append(current_status)

                    else:
                        while check_end != '':

                            species_com.append(name_list[counter_a])
                            sci = (name_list[counter_a + 1].replace('(', ''))
                            sci = sci.replace(')', '')
                            species_sci.append(sci)
                            group.append(current_group)
                            pop.append(pop_name)
                            year.append(str(column_2))
                            col = row.find_all('td')
                            column_3 = col[1].get_text()

                            column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')

                            check_fr = column_3.split(' ')
                            # print check_fr
                            if 'FR' in check_fr:
                                current_status = 'candidate'
                            else:
                                current_status = column_3
                            status.append(current_status)
                            counter_a += 2
                            check_end = name_list[counter_a]
            else:
                species_com.append(name_list[0])
                sci = (name_list[1].replace('(', ''))
                sci = sci.replace(')', '')
                if sci == '':  ## TODO need to account for the bullet species that have two blanks
                    print name_list
                # and append it to first_name variable
                species_sci.append(sci)
                group.append(current_group)
                pop.append(pop_name)

                # and append it to last_name variable
                year.append(str(column_2))

                # Create a variable of the string inside 3rd <td> tag pair,
                column_3 = col[1].get_text()
                column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')

                check_fr = column_3.split(' ')
                # print check_fr
                if 'FR' in check_fr:
                    current_status = 'candidate'
                else:
                    current_status = column_3
                # and append it to age variable
                status.append(current_status)
        elif len(col) == 3:

            column_1 = col[0].get_text()
            name_list = (column_1).split("\n")
            print name_list
            sci = (name_list[1].replace('(', ''))
            sci = sci.replace(')', '')

            if sci == '6 elasmobranch species':
                total_list = len(name_list) - 1
                counter = 3
                while counter < total_list:
                    com_name = name_list[counter]
                    sci_name = name_list[counter + 1]
                    sci = (sci_name.replace('(', ''))
                    sci = sci.replace(')', '')

                    species_com.append(com_name)
                    species_sci.append(sci)
                    group.append(current_group)
                    pop.append(pop_name)

                    # and append it to last_name variable
                    column_2 = col[1].get_text()
                    year.append(str(column_2))

                    # Create a variable of the string inside 3rd <td> tag pair,
                    column_3 = col[2].get_text()
                    column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')

                    check_fr = column_3.split(' ')

                    if 'FR' in check_fr:
                        current_status = 'candidate'
                    else:
                        current_status = column_3
                    status.append(current_status)
                    counter += 2
            elif sci == "dolphin, Hector's 2 subspecies":

                com_name = name_list[1]
                sci_name = name_list[5]
                pop_name = name_list[4]
                sci = (sci_name.replace('(', ''))
                sci = sci.replace(')', '')

                species_com.append(com_name)
                species_sci.append(sci)
                pop.append(pop_name)
                group.append(current_group)

                # and append it to last_name variable
                column_2 = col[1].get_text()
                year.append(str(column_2))

                # Create a variable of the string inside 3rd <td> tag pair,
                column_3 = col[2].get_text()
                column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')

                check_fr = column_3.split(' ')

                if 'FR' in check_fr:
                    current_status = 'candidate'
                else:
                    current_status = column_3
                status.append(current_status)

            elif sci == "whale, Bryde's 1 subspecies":
                print name_list

                com_name = name_list[1]

                pop_name = name_list[4]
                sci_name = name_list[2]
                sci = (sci_name.replace('(', ''))
                sci = sci.replace(')', '')

                species_com.append(com_name)
                species_sci.append(sci)
                pop.append(pop_name)
                group.append(current_group)

                # and append it to last_name variable
                column_2 = col[1].get_text()
                year.append(str(column_2))

                # Create a variable of the string inside 3rd <td> tag pair,
                column_3 = col[2].get_text()
                column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')

                check_fr = column_3.split(' ')

                if 'FR' in check_fr:
                    current_status = 'candidate'
                else:
                    current_status = column_3
                status.append(current_status)

            else:
                if len(name_list) > 2:

                    pop_name = name_list[3]
                    sci_name = name_list[4]
                    sci = (sci_name.replace('(', ''))
                    sci = sci.replace(')', '')

                    species_com.append(name_list[0])
                    species_sci.append(sci)
                    pop.append(pop_name)
                    group.append(current_group)
                else:
                    species_com.append(name_list[0])
                    species_sci.append(sci)
                    pop.append(pop_name)
                    group.append(current_group)

                column_2 = col[1].get_text()

                # and append it to last_name variable
                year.append(str(column_2))

                # Create a variable of the string inside 3rd <td> tag pair,
                column_3 = col[2].get_text()
                column_3 = column_3.encode('ascii', 'replace').replace('?', ' ')
                check_fr = column_3.split(' ')
                # print check_fr

                if 'FR' in check_fr:
                    current_status = 'candidate'
                else:
                    current_status = column_3
                # and append it to age variable
                status.append(current_status)

# print len(species_sci)
# print len(species_com)
# print len(status)
# print len(year)
# print len(group)
# print len(pop)
# print len(current_group)
columns = {'Invname': species_com, 'Scientific Name': species_sci, 'Status': status, 'Year Listed': year,
           'Group_B': group, 'Population': pop, 'Critical Habitat': ['n/a'] * len(species_com),
           'Recovery Plan': ['n/a'] * len(species_com)}
df = pd.DataFrame(columns,
                  columns=['Invname', 'Scientific Name', 'Population', 'Year Listed', 'Status', 'Critical Habitat',
                           'Recovery Plan', 'Group_B'])
print df
remove_blanks = df.loc[df['Invname'].isin(['']) == False]
remove_foreign = remove_blanks[remove_blanks['Scientific Name'].isin(removed_perNMFS) == False]

remove_foreign.to_csv(outlocation + os.sep + 'FilteredWebsite_NMFS_PropCan' + date + '.csv', encoding='utf-8')
df.to_csv(outlocation + os.sep + 'FullWebsite_NMFS_PropCan' + date + '.csv', encoding='utf-8')
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
