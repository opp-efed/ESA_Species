__author__ = 'JConno02'
import os
import datetime
import pandas as pd

##Todo rather than loading in dict add a field to df that is in out and pop field based on if ent in on filter list then select all rows that are in
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

inpath_FULLTESS = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\Feb2017\FWS\FullTess_20170221.csv'
previos_FWS = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'
family_group_cross ='C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Family_Group_crosswalk_20170325.csv'
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\March2017\FWS'


## These are the variables use within the xml, if FWS changes their variable this will need to be updated

identifier = 'entity_id'
out_cols = ['entity_id', 'Notes', 'comname', 'sciname', 'invname', 'status', 'status_text', 'pop_abbrev',
            'pop_desc', 'family', 'spcode', 'vipcode', 'lead_agency', 'country', 'Group']

current_master_col = [u'EntityID', u'Notes', u'comname', u'sciname', u'invname', u'status', u'status_text',
                      u'pop_abbrev',
                      u'pop_desc', u'family', u'spcode', u'vipcode', u'lead_agency', u'country', u'Group']
# Tess has species tagged to the wrong lead agency should be both (3)11191 green sea turtle, 10381 bearded seal
# 397 id the genus listing of the Oahu trees snail and the individual pops are consider, the are add at line 112 also
# experiemental wood bison add
wrong_lead = ['11191', '10381','397']


# Uses user prompts to filter the full list down to the section 7 species that should be considered
def filterquery(df, header):
    start = datetime.datetime.now()
    possAnswer = ['Yes', 'No']
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

dfFullTess = pd.read_csv(inpath_FULLTESS)
dfFullTess = dfFullTess.reindex(columns=out_cols)
header = list(dfFullTess.columns.values)

fmy_grp_xwalk = pd.read_csv(family_group_cross)

master_list_df = pd.read_excel(previos_FWS)
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: str(x))
current_fws_df = master_list_df.loc[master_list_df['lead_agency'] == 1]

master_list_df_filter = master_list_df.reindex(columns=current_master_col)
tree_snails_pop = master_list_df_filter.loc[master_list_df_filter['comname'] == 'Oahu tree snail']
wood_bision = master_list_df_filter.loc[master_list_df_filter['EntityID'] == 'FWS001']
tree_snails_pop.columns = out_cols
wood_bision.columns = out_cols


filter_df = filterquery(dfFullTess, header)

filter_df['entity_id'] = filter_df['entity_id'].map(lambda x: str(x))
for v in wrong_lead:
    filter_df.loc[filter_df['entity_id'] == v, 'lead_agency'] = 3

filter_df = pd.concat([filter_df, tree_snails_pop], axis=0)
filter_df = pd.concat([filter_df, wood_bision], axis=0)
filter_df['Group'] = filter_df.apply(lambda row: check_group(row, fmy_grp_xwalk), axis=1)


outfilter = outpath + os.sep + 'FilteredTessPandas_' + str(date) + '.csv'
filter_df.to_csv(outfilter, encoding='utf-8')
filterents = filter_df["entity_id"].tolist()

print "There are {0} listed entities that will be considered".format(len(filterents))

end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()
print "Elapsed  Time: " + str(end - start_time)

print '\nCheck that the following columns are complete before merging with nmfs and ' \
      'checking for updates \n {0} \n Family may need to be added manually'.format(out_cols)
