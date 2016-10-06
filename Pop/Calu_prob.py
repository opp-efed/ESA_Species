import pandas as pd

masterlist= 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\FinalLists\FinalBE_December2016\csv\MasterListESA_June2016_20160907.csv'
overlap_table ='C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Overlap\overlap_fromWOE.csv'
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Range_Filename', 'Des_CH', 'CH_GIS',
                'CH_Filename']

def PickSpecies (master_list):
    yes_no =['Yes','No']
    QA_species = True
    while QA_species:
        if master_list.split('.')[1]== 'csv':
            master_list_df = pd.read_csv(master_list)
        else:
            master_list_df = pd.read_excel(master_list)

        master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
        sp_info_df = pd.DataFrame(master_list_df, columns=col_included)

        entlist = master_list_df['EntityID'].values.tolist()

        user_input = raw_input('What species are you looking at: ')
        if user_input not in entlist:
                print ('This is not a valid answer')
        else:
            QA_species = False
            sp_info_included = sp_info_df[sp_info_df['EntityID'].isin([user_input]) == True]
            print sp_info_included
            QA2 = True
            while QA2:
                user_input2= raw_input('\nIs this the correct species?:' )
                if user_input2 not in yes_no:
                    print 'This is not a valid answer must be Yes or No'
                else:
                    QA2 = False
                    if user_input2 == 'Yes':
                        pass
                    else:
                        QA_species = True
        species_category = ['Terrestrial', 'Aquatic']
        QA_category = True
        while QA_category:
            user_input3 = raw_input('Is your species Terrestrial or Aquatic?: ')
            if user_input3 not in species_category:
                print 'This is not a valid answer'
            else:
                QA_category = False
                pass
        QA_distribution =True
        while QA_distribution:
            user_input4= raw_input('Do you want to assume a uniform distribution?: ')
            if user_input4 not in yes_no:
                print 'This is not a valid answer must be Yes or No'
            else:
                QA_distribution = False


    return sp_info_included, user_input3, user_input4, user_input

def ExtractOverlap (overlap_input,spe):
    if overlap_input.split('.')[1]== 'csv':
        overlap_df = pd.read_csv(overlap_input)
    else:
        overlap_df = pd.read_excel(overlap_input)
    overlap_df['EntityID'] = overlap_df['EntityID'].astype(str)
    print spe
    print overlap_df


    overlap_sp = overlap_df[overlap_df['EntityID'].isin([spe]) == True]
    return overlap_sp

def PercentMortality(entityid, overlap, threshold):
    listSlopes=[]
    listConcentrations =[]

species, spe_cat, distribution, entityID = PickSpecies(masterlist)
print 'Working on {0}..with a {2} distribution... {1}'.format(spe_cat, species, distribution)
overlap = ExtractOverlap(overlap_table, entityID)
print overlap


