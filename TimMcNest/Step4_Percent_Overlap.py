import os
import pandas as pd
import datetime
import arcpy

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

infolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Tables_Fall2016\Agg_layers\NonAg\SumBySpecies'
##export att table of refuge clips of hab
outlocation = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Tables_Fall2016\Agg_layers\NonAg\PercentOverlap'
in_clipped_raster = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Clipped_GAP.gdb'

speHabitDict = {'94': [286, 38, 97, 90],
                '123': [39, 41, 42, 45, 56, 148, 179, 277, 278, 281, 282, 296, 297, 298, 300, 301, 302, 303, 304, 305,
                        358, 359, 360, 556, 557, 558, 559, 562, 563, 581, 582, 583],
                '133': [290, 291, 292, 293, 294, 295, 363, 370, 379, 407, 408, 410, 411, 449, 556, 557, 574, 575],
                '145': [296, 297, 298, 300, 302, 303, 359, 360, 383, 384, 385, 470, 471, 472, 476, 485, 489],
                '6901': [39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 54, 55, 56, 57, 58, 136, 137, 138, 139, 140, 141,
                         142, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 158, 159, 160, 161, 162,
                         163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 181, 182,
                         183, 184, 185, 186, 187, 188, 189, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276,
                         277, 278, 279, 280, 281, 282, 356, 357, 358, 359, 360, 361, 424, 425, 427, 555],

                '10147': [333, 335, 337, 422, 424, 426,325,334,336,416,567,572], # added 325-572 on 1/26/2017
                '89': [539, 541, 553, 460, 461, 462, 466, 467, 468, 469, 472, 473, 474, 475, 476, 477, 358, 443, 444],
                '139': [30, 46, 50, 51, 52, 53, 59, 65, 70, 91, 104, 188, 193, 197, 198, 200, 222, 223, 235, 240, 253,
                        281, 283, 284],
                '149': [39, 40, 41, 42, 43, 45, 46, 47, 48, 49, 55, 56, 145, 146, 148, 49, 151, 152, 153, 154, 155, 156,
                        158, 159, 162, 163, 164, 165, 175, 179, 181, 183, 184, 185, 186, 187, 188, 189, 194, 266, 270,
                        271, 272, 277, 278, 280, 281, 282, 296, 297, 298, 300, 301, 302, 303, 304, 305, 309, 315, 316,
                        317, 323, 326, 329, 330, 331, 356, 357, 358, 359, 360, 361, 383, 384, 385, 432, 433, 438, 439,
                        442, 443, 444, 445, 455, 457, 458, 459, 509, 578, 579],
                '83': [556, 557, 20, 30, 70, 222, 223, 233, 235, 284, 560, 562, 479, 480, 481, 482, 483, 339, 340, 372,
                       378, 392, 393, 415, 452, 454],

                '138': [30, 46, 47, 50, 51, 52, 53, 59, 65, 70, 91, 104, 188, 189, 192, 193, 197, 198, 200, 222, 223,
                        235, 240, 253, 271, 281, 283, 284, 317, 324, 328, 329, 330, 331, 338, 339, 358, 392, 426, 442,
                        459, 460, 461, 462, 463, 464, 465, 467, 468, 469, 475, 479, 481, 482, 483, 556, 557, 558, 559,
                        560],
                '137': [444, 457, 459, 470, 471, 472, 476, 477, 489, 581],
                '116': [303, 304, 385, 432],
                '4064': [315, 316, 317, 323, 326, 438, 439, 502, 503, 556, 558, 559],

                }

def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)
def extract_acres_preferred_habitat(in_fc, entid, preferred_hab):
    att_array = arcpy.da.TableToNumPyArray((in_fc), ['Value', 'Count'])

    att_df = pd.DataFrame(data=att_array)
    total_sum =att_df['Count'].sum()

    pref_value = att_df['Value'].values.tolist()
    count_value = att_df['Count'].values.tolist()
    cout_df = pd.DataFrame(data=count_value)
    transformed = cout_df.T
    transformed.columns = pref_value

    cols_preferred = []
    for col in pref_value:
        try:
            if int(col) in preferred_hab:
                cols_preferred.append(col)
            else:
                pass
        except:
            pass

    preferred_df = transformed.ix[:, cols_preferred].apply(pd.to_numeric)

    preferred_df["sum"] = preferred_df.sum(axis=1)
    sum_pref = preferred_df.ix[0, 'sum']
    msq_conversion = 30 * 30
    msq_pre = sum_pref * msq_conversion
    acres_pref = msq_pre * (0.000247)

    msq_full = total_sum * msq_conversion
    acres_full = msq_full* (0.000247)
    return acres_pref, acres_full


def summarize_species(listfolder, multi, multi_break, acres_pref, acres_full):

    csv_list_species = [csv for csv in listfolder if str(csv.split("_")[2]) == entid]
    if multi:
        if multi_break != 'breeding':
            if multi_break != 'migration':
                csv_list_species=['SummaryTable_Count_94_'+str(date)+'.csv']

        else:
            csv_list_species = [csv for csv in csv_list_species if str(csv.split("_")[3]) == multi_break]
    for csv in csv_list_species:
        in_use_df = pd.read_csv((infolder + os.sep + csv))

        in_use_df = in_use_df.reindex(
            columns=['Use', 'CDL Year', 'Spray Drift','Count_Full', 'MSQ_Full', 'Use_Acres_Full', 'Total Acres Full',
                     'Percent Overlap Full', 'Count_Suit', 'MSQ_Suit', 'Use_Acres_Suit', 'Total Acres Suitable',
                     'Percent Overlap Suitable'])

        # sum_df =sum_df .iloc[:, (sp_col_count + 1):(len(header_sp))].apply(pd.to_numeric)
        in_use_df['MSQ_Full'] = in_use_df['Count_Full'].multiply(900)
        in_use_df['Use_Acres_Full'] = in_use_df['MSQ_Full'].multiply(0.000247)
        in_use_df['Total Acres Full'].fillna(acres_full, inplace=True)
        in_use_df['Percent Overlap Full'] = (in_use_df['Use_Acres_Full'].div(in_use_df['Total Acres Full'])) * 100

        in_use_df['MSQ_Suit'] = in_use_df['Count_Suit'].multiply(900)
        in_use_df['Use_Acres_Suit'] = in_use_df['MSQ_Suit'].multiply(0.000247)
        in_use_df['Total Acres Suitable'].fillna(acres_pref, inplace=True)
        in_use_df['Percent Overlap Suitable'] = (in_use_df['Use_Acres_Suit'].div(in_use_df['Total Acres Suitable'])) * 100
    out_csv = csv.replace('Count', 'PercentOverlap')
    in_use_df.to_csv(outlocation + os.sep + out_csv)
    print 'Exported table for species {0}, {1}'.format(entid, species_range_break)


listfolder = os.listdir(infolder)

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
createdirectory(outlocation)
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

entList = speHabitDict.keys()

for entid in entList:
    preferred_hab_values = speHabitDict[entid]
    if entid == '94':
        break_species = True
        for species_range_break in ['migration', 'breeding','']:
            if species_range_break =='':
                infc = in_clipped_raster + os.sep + 'natgaplandcov_v2_2_1_clip_' + str(entid)
                pref_acres,full_acres = extract_acres_preferred_habitat(infc, entid, preferred_hab_values)
            else:
                infc = in_clipped_raster + os.sep + 'natgaplandcov_v2_2_1_clip_' + str(entid) + "_" + species_range_break
                pref_acres,full_acres  = extract_acres_preferred_habitat(infc, entid, preferred_hab_values)
    else:
        break_species = False
        infc = in_clipped_raster + os.sep + 'natgaplandcov_v2_2_1_clip_' + str(entid)
        pref_acres,full_acres = extract_acres_preferred_habitat(infc, entid, preferred_hab_values)

    list_all_csv = os.listdir(infolder)
    list_all_csv = [csv for csv in list_all_csv if csv.endswith('csv')]
    if break_species:
        for species_range_break in ['migration', 'breeding','']:
            out_df = summarize_species(list_all_csv, break_species, species_range_break, pref_acres, full_acres)

    else:
        species_range_break = 'nan'
        out_df = summarize_species(list_all_csv, break_species, species_range_break, pref_acres,full_acres)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
