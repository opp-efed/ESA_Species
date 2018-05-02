import datetime
import os
import sys
import pandas as pd
import requests
from bs4 import BeautifulSoup
import copy


#TODO add an output that merges all table to be used in following steps

# Description: J. Connolly 3/23/2017
# Loop through each tables and extract species information into the standard columns with all import information
# #################### VARIABLES
# #### user input variables

outlocation = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\MasterLists\Creation\test'  # path final tables
# Species groups used by NMFS
groups = ['Cetaceans', 'Pinnipeds', 'Sea Turtles', 'Other Marine Reptiles', 'Corals', 'Abalone', 'Fishes',
          'Sea Turtles', 'Other Marine Reptiles']
# NMFS website with the tables of listed species
url = "http://www.nmfs.noaa.gov/pr/species/esa/listed.htm"
# foreign species per NMFS but marked as domestic on website
removed_perNMFS =['Pristis pristis formerly P. perotteti, P. pristis, and P. microdon']
# statuses that will be included when final table is filtered
# Note: NMFS experimental populations do not fall under section 7, see notes from T Hooper
section_7_status = ['E', 'T']


def tables_read(tables_html, n):
    n_cols = 0
    n_rows = 0

    # Parse each table

    for row in tables_html[n].find_all("tr"):
        col_tags = row.find_all(["td", "th"])
        if len(col_tags) > 0:
            n_rows += 1
            if len(col_tags) > n_cols:
                n_cols = len(col_tags)

        # Create dataframe
    df = pd.DataFrame(index=range(0, n_rows), columns=range(0, n_cols))

    # Create list to store rowspan values
    skip_index = [0 for i in range(0, n_cols)]

    # Start by iterating over each row in this table...
    row_counter = 0
    for row in tables_html[n].find_all("tr"):

        # Skip row if it's blank
        if len(row.find_all(["td", "th"])) == 0:
            next

        else:

            # Get all cells containing data in this row
            columns = row.find_all(["td", "th"])
            col_dim = []
            row_dim = []
            col_dim_counter = -1
            row_dim_counter = -1
            col_counter = -1
            this_skip_index = copy.deepcopy(skip_index)
            # ROWSPAN AND COLSPAN CHECKS FOR MERGED CELL WHICH ARE TYPICALLY NOT CONSISTENCE ON WEBSITE SEE REFERNCE
            # WEBSITE FOR MORE DETAILS
            for col in columns:

                # Determine cell dimensions
                colspan = col.get("colspan")
                if colspan is None:
                    col_dim.append(1)
                else:
                    col_dim.append(int(colspan))
                col_dim_counter += 1

                rowspan = col.get("rowspan")
                if rowspan is None:
                    row_dim.append(1)
                else:
                    row_dim.append(int(rowspan))
                row_dim_counter += 1

                # Adjust column counter
                if col_counter == -1:
                    col_counter = 0
                else:
                    col_counter = col_counter + col_dim[col_dim_counter - 1]

                while skip_index[col_counter] > 0:
                    col_counter += 1

                # Get cell contents
                cell_data = col.get_text().encode('ascii', 'replace').replace('\n', '').replace('?', ' ')

                # Insert data into cell
                df.iat[row_counter, col_counter] = cell_data

                # Record column skipping index
                if row_dim[row_dim_counter] > 1:
                    this_skip_index[col_counter] = row_dim[row_dim_counter]

        # Adjust row counter
        row_counter += 1

        # Adjust column skipping index
        skip_index = [i - 1 if i > 0 else i for i in this_skip_index]

    # Append dataframe to list of tables
    tables_html.append(df)

    return (df)


def get_tables(htmldoc):
    soup = BeautifulSoup(htmldoc.content, 'html.parser')
    h2 = soup.find_all('h2')
    title = []
    for v in h2:
        t = v.getText().encode('ascii', 'replace').replace('\n', '').replace('?', ' ')
        title.append(t)
    return soup.find_all('table'), title


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


def standard_groups(row):
    group = row['Group']

    if group =='Fish':
        return 'Fishes'
    elif group == 'Invertebrates':
        if row['Sub_Group'] == 'Corals':
            return 'Corals'
        else:
            return 'Aquatic Invertebrates'
    else:
        return group


def standard_year(row):
    year = str(row['Year Listed'])
    recent_year = year.split(" ")
    if recent_year[0] == 'nan' or recent_year[0] =='':
        return 'nan'
    else:
        return recent_year[0]


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

createdirectory(outlocation+os.sep+'NMFS')
outlocation = outlocation+os.sep+'NMFS'

# Step 1:Download and parse out website tables
r = requests.get(url)

list_tables, title = get_tables(r)
print

# Step 2: For each table parse out species information information into a dataframe from html them extract values to the
# specific column headers we need
for n in range(0, len(list_tables)):
    ssa = tables_read(list_tables, n)
    # ssa.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\MasterLists\Creation\test\NMFS\tables_'+ str(n)+".csv")
    ssa.columns = ssa.iloc[0]
    ssa= ssa.reindex(ssa.index.drop(0))
    parent_group = str(title[n].split('(')[0].strip()).split(" ")[len(str(title[n].split('(')[0].strip()).split(" "))-1]
    group = ''

    ssa['Common Name or Population'] = ssa['Species'].map(
        lambda x: str(x.split('(')[0].split(',')[1]).strip() + " " + str(x.split('(')[0].split(',')[0]).strip() if len(
            x.split('(')[0].split(',')) >= 2 else str(x.split('(')[0].split(',')[0]))
    ssa['Scientific Name'] = ssa['Species'].map(lambda x: x if len(x.split('(')) <= 1 else (
        str(x.split('(')[1]).replace(')', '').strip()) if len(x.split('(')) == 2 else str(
        (x.split('(')[2]).split(')')[0]).replace(')', '').strip())

    row_count = 1

    while row_count <= len(ssa):
        if str(ssa.ix[row_count,'Species']).strip() in groups:
            group = ssa.ix[row_count,'Species'].strip()
            row_count +=1
        else:
            ssa.ix[row_count,'Group'] = parent_group
            ssa.ix[row_count,'Sub_Group'] = group
            row_count +=1
    ssa['Group'] = ssa.apply(lambda row: standard_groups(row), axis=1)
    ssa['Year Most Recent'] = ssa.apply(lambda row: standard_year(row), axis=1)

    row_count = 1

    while row_count <= len(ssa):
        previous_row = int(row_count -1)
        if (ssa.ix[row_count, 'Year Most Recent']) == 'nan':
            sci_name = ssa.ix[row_count, 'Scientific Name'].strip()
            common_name = ssa.ix[row_count, 'Common Name or Population'].strip()

        if str(ssa.ix[row_count,'Species']).strip() in groups:
            pass
        elif (ssa.ix[row_count, 'Scientific Name']).strip() == (ssa.ix[row_count, 'Species']).strip():
            ssa.ix[row_count, 'Population'] = ssa.ix[row_count, 'Common Name or Population']
            ssa.ix[row_count, 'Scientific Name'] = sci_name
            ssa.ix[row_count, 'Common Name'] = common_name

        elif (ssa.ix[row_count, 'Scientific Name']).strip().startswith(sci_name):
            ssa.ix[row_count, 'Common Name'] = common_name
            ssa.ix[row_count, 'Population'] = ssa.ix[row_count, 'Common Name or Population'].strip()
        else:
            if ssa.ix[row_count, 'Scientific Name'] == 'SONCC':  #Hard wired exception due to formatting of website
                ssa.ix[row_count, 'Scientific Name'] = 'Oncorhynchus kisutch'
                ssa.ix[row_count, 'Common Name'] = 'coho salmon'
                ssa.ix[row_count, 'Population'] = 'Southern Oregon & Northern California coasts'
            else:
                ssa.ix[row_count, 'Common Name'] = ssa.ix[row_count, 'Common Name or Population'].strip()
                ssa.ix[row_count, 'Population'] = 'None'
        row_count +=1

    ssa['Group'].fillna(0, inplace =True)


# Step 3: Filters data frame to include only the statuses of concern for section 7, removes empty rows, or rows with
# non-species information
    remove_blanks = ssa.loc[ssa['Group'] != 0]
    remove_foreign = remove_blanks[remove_blanks['Scientific Name'].isin(removed_perNMFS) == False]
    remove_foreign = remove_foreign.loc[remove_foreign['Status'].isin(section_7_status) == True]
# Step 4: Exports both the full list from the website and filtered list to csvs
# Exports two tables, the first is a complete list of species from website table, second is only the section 7 species.
    remove_foreign.to_csv(outlocation + os.sep + 'FilteredWebsite_NMFS_'+ title[n].split('(')[0] +"_"+ date + '.csv', encoding='utf-8')
    ssa.to_csv(outlocation + os.sep + 'FullWebsite_NMFS_'+ title[n].split('(')[0] +"_" + date + '.csv', encoding='utf-8')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)
