__author__ = 'JConno02'
__author__ = 'JConno02'

from bs4 import BeautifulSoup
import requests
import sys
import os
import datetime
import pandas as pd

import csv

##Todo rather than loading in dict add a field to df that is in out and pop field based on if ent in on filter list then select all rows that are in
date = 20160607
inpath_FULLTESS =r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas\FullTess_20160607.csv'
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas'
url = "http://ecos.fws.gov/services/TessQuery?request=query&xquery=/SPECIES_DETAIL"

r = requests.get(url)
reload(sys)
sys.setdefaultencoding('utf8')
## These are the variables use within the xml, if FWS changes their variable this will need to be updated

identifier='entity_id'
deafultTags = ['spcode', 'vipcode', 'sciname', 'comname', 'invname', 'pop_abbrev', 'pop_desc', 'family', 'status',
               'status_text', 'lead_agency', 'lead_region', 'country', 'listing_date', 'dps', 'refuge_occurrence',
               'delisting_date']

# Uses user prompts to filter the full list down to the section 7 species that should be considered
def filterquery(df,header):

    possAnswer = ['Yes', 'No']
    askQ = True
    while askQ:
        status = raw_input('Where is the status located {0}: '.format(header))

        if status not in header:
            print 'This is not a valid answer: remove quotes and spaces'
        else:
            liststatus =df[status].tolist()
            break

    while askQ:
        country = raw_input('Where is the country  located {0}: '.format(header))
        if country not in header:
            print 'This is not a valid answer: remove quotes and spaces'
        else:
            break
    Failed = False
    while askQ:
        StatusConsidered = ['Experimental Population  Non-Essential', 'Threatened', 'Endangered', 'Proposed Threatened',
                            'Proposed Endangered', 'Candidate']

        defaultStatus = raw_input(
            'The current status being considered are:  \n{0}\nWould you like to add additional statuses? : Yes or No: '.format(
                StatusConsidered))

        if defaultStatus not in possAnswer:
            print('This is not a valid answer, must be Yes or No')
        else:
            if defaultStatus == 'Yes':
                filtered_status = raw_input('Which statuses should be included in the filtered table{0}? '.format(
                    liststatus))  ##TODO print out list one value per line
                listinput = filtered_status.split(",")
                for v in listinput:
                    if v not in liststatus:
                        print v
                        Failed = True
                        print 'This is not a valid answer: values must be separated by a comma without a space'
                    else:
                        if v not in StatusConsidered:
                            StatusConsidered.append(v)

            else:
                if Failed == False:
                    askQ = False
                else:
                    askQ = True
    countrylist = ['1','3']
    start = datetime.datetime.now()
    statusfilter =df.loc[df[status].isin(StatusConsidered)]
    filter = statusfilter.loc[statusfilter[country].isin(countrylist)]
    print 'Completed filter in {0}'.format((datetime.datetime.now()-start))

    outfilter = outpath + os.sep + 'FilteredTessPandasbb_' + str(date) + '.csv'
    filter.to_csv(outfilter, encoding='utf-8')
    return filter

dfFullTess = pd.read_csv(inpath_FULLTESS)
header= list(dfFullTess.columns.values)

filter_df = filterquery(dfFullTess, header)

outfilter = outpath + os.sep + 'FilteredTessPandas_' + str(date) + '.csv'

filter_df.to_csv(outfilter, encoding='utf-8')
filterents = filter_df["entity_id"].tolist()
print "There are {0} listed entities that will be considered".format(len(filterents))

#Pandas code to pop a col then filter based on that- queury method and general index (old method pre-query
#     while row < (rowindex):
#         entid =str(df.iloc[row, entidindex])
#         print "Working on species {0}, row {1}".format(entid, row)
#         status = str(df.iloc[row, status_index])
#
#         country =str(df.iloc[row, country_index])
#
#         if status in StatusConsidered:
#             if country in countrylist:
#                 df[row,'Filter'] = 'Yes'
#                 print 'Made a Yes for status {0} and country {1}'.format(status,country)
#             else:
#                 df[row,'Filter'] = 'No'
#         else:
#             df[row,'Filter'] = 'No'
#         row+=1
#
#     print df
#     outfilter = outpath + os.sep + 'FilteredTessPandasbb_' + str(date) + '.csv'
#     df.to_csv(outfilter, encoding='utf-8')
#     filterdf= header.append('Filter')
#     df.loc[df['Filter']=='Yes']
#     df.query['Filter==Yes']
#     print filterdf
#
#
#
#     return filterdf, header