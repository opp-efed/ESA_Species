import os
import datetime
import pandas as pd

__author__ = 'JConno02'


outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\April2017'
inpath_FULLTESS = r'FullTess_20170410.csv'

previos_FWS = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'
family_group_cross ='C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Family_Group_crosswalk_20170325.csv'

## These are the variables use within the xml, if FWS changes their variable this will need to be updated

identifier = 'EntityID'
out_cols = ['EntityID', 'Notes', 'comname', 'sciname', 'invname', 'status', 'status_text', 'pop_abbrev',
            'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country', 'Group']
# index location must be in the same location in both lists
entid_index = 0
lead_agency_index =12
group_index =14

current_master_col = [u'EntityID', u'Notes', u'comname', u'sciname', u'invname', u'status', u'status_text',
                      u'pop_abbrev',u'pop_desc', u'family', u'spcode', u'vipcode', u'lead_agency', u'country', u'Group']

# Tess has species tagged to the wrong lead agency should be both (3)11191 green sea turtle, 1
# 397 id the genus listing of the Oahu trees snail and the individual pops are consider, the are add at line 112 also
# experiemental wood bison add
wrong_lead = ['11191','397']

# Species removed from TESS that should be included
# species removed in TESS but should be included xn wood bision and indiv. pops of the oahu tree snail, tess has it
# group to genus
not_found_inTESS =['FWS001','9395',	'9397',	'9399',	'9401',	'9403',	'9405',	'9407',	'9409',	'9411',	'9413',	'9415',
                  '9417',	'9419',	'9421',	'9423',	'9433',	'9435',	'9437',	'9439',	'9441',	'9443',	'9445',	'9447',
                  '9449',	'9451',	'9453',	'9455',	'9457',	'9459',	'9461',	'9463',	'9465',	'9467',	'9469',	'9471',
                  '9473',	'9475',	'9477',	'9479',	'9481',	'9483']
# Uses user prompts to filter the full list down to the section 7 species that should be considered
def filterquery(df, header):
    start = datetime.datetime.now()
    possAnswer = ['Yes', 'No']
    ask_preQ = True
    while ask_preQ:
        default_answers = raw_input('Would you like to use the default answers, status_text, country, section 7 status {0}: '.format(possAnswer))

        if default_answers  not in possAnswer:
            print 'This is not a valid answer: remove quotes and spaces'
        else:
            status = 'status_text'
            country = 'country'
            StatusConsidered = ['Experimental Population  Non-Essential', 'Threatened', 'Endangered', 'Proposed Threatened',
                            'Proposed Endangered', 'Candidate']
            break
    if default_answers == 'No':
        askQ = True
        while askQ:
            status = raw_input('Where is the status_text located {0}: '.format(header))

            if status not in header:
                print 'This is not a valid answer: remove quotes and spaces'
            else:
                liststatus = df[status].tolist()
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
                '\nThe current status being considered are:  \n{0}\nWould you like to add additional statuses? : Yes or No: '.format(
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
    else:
        pass
    countrylist = ['1', '3']


    statusfilter = df.loc[df[status].isin(StatusConsidered)]
    filter = statusfilter.loc[statusfilter[country].isin(countrylist)]
    print 'Completed filter in {0}'.format((datetime.datetime.now() - start))
    return filter


def check_group(row, fws_current_df):
    family = str(row['family']).title()
    try:
        group_value = str(fws_current_df.loc[fws_current_df['family'] == family, 'Group'].iloc[0])
        return group_value
    except:
        pass


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

outpath = outpath+os.sep+'FWS'
inpath_FULLTESS =outpath+os.sep+inpath_FULLTESS
dfFullTess = pd.read_csv(inpath_FULLTESS)
dfFullTess = dfFullTess.reindex(columns=out_cols)
header = list(dfFullTess.columns.values)

fmy_grp_xwalk = pd.read_csv(family_group_cross)

master_list_df = pd.read_excel(previos_FWS)
master_list_df[current_master_col[entid_index]] = master_list_df[current_master_col[entid_index]].map(lambda x: str(x))
current_fws_df = master_list_df.loc[master_list_df[current_master_col[lead_agency_index] ] == 1]

master_list_df_filter = master_list_df.reindex(columns=current_master_col)
missing_from_tess = master_list_df_filter.loc[master_list_df_filter[current_master_col[entid_index]].isin(not_found_inTESS)]
missing_from_tess.columns = out_cols


filter_df = filterquery(dfFullTess, header)
filter_df[out_cols[entid_index]] = filter_df[out_cols[entid_index]].map(lambda x: str(x))
filter_df[out_cols[lead_agency_index]] = filter_df[out_cols[lead_agency_index]].map(lambda x: str(x))

print filter_df.columns.values.tolist()
for v in wrong_lead:
    filter_df.loc[filter_df[out_cols[entid_index]] == v,out_cols[lead_agency_index]] = 3

filter_df = pd.concat([filter_df, missing_from_tess], axis=0)
filter_df[out_cols[group_index]] = filter_df.apply(lambda row: check_group(row, fmy_grp_xwalk), axis=1)


outfilter = outpath + os.sep + 'FilteredTessPandas_' + str(date) + '.csv'
filter_df.to_csv(outfilter, encoding='utf-8')
filterents = filter_df[out_cols[entid_index]].tolist()

print "There are {0} listed entities that will be considered".format(len(filterents))

end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)

print '\nCheck that the following columns are complete before merging with nmfs and ' \
      'checking for updates \n {0}'.format(out_cols)