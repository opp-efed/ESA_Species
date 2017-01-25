import os
import pandas as pd
import datetime

in_ag_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Tables_Fall2016\Ind_year_drift\PercentOverlap'
in_nonag_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Tables_Fall2016\Agg_layers\NonAg\PercentOverlap'
outfolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Tables_Fall2016\merge_ag_nonag'
date_onfile = '20170125.csv'
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

                '10147': [333, 335, 337, 422, 424, 426],
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
start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)
entlist = speHabitDict.keys()
list_ag = os.listdir(in_ag_folder)
list_nonag = os.listdir(in_nonag_folder)


def merge_csv(outfolder, ag_list, non_ag_list):
    if len(ag_list)>1:
        print 'Multiple csv in ag list'
    if len(non_ag_list)>1:
        print 'Multiple csv in non ag list'
    in_ag_df = pd.read_csv(in_ag_folder + os.sep + ag_list[0] )
    in_nonag_df = pd.read_csv(in_nonag_folder + os.sep + non_ag_list[0] )
    out_df = pd.concat([in_ag_df, in_nonag_df], axis=0)
    out_df .drop('Unnamed: 0', axis=1, inplace=True)
    out_df.to_csv(outfolder + os.sep + str(ag_list[0]),index = False)


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
createdirectory(outfolder)
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

for entid in entlist:

    sp_ag = [csv for csv in list_ag if csv.split("_")[2] == entid]
    sp_nonag = [csv for csv in list_nonag if csv.split("_")[2] == entid]
    if entid == '94':
        for species_range_break in ['migration', 'breeding', '']:
            if species_range_break == '':
                sp_ag = [csv for csv in list_ag if csv.split("_")[2] == entid]
                sp_nonag = [csv for csv in list_nonag if csv.split("_")[2] == entid]

                sp_ag = [csv for csv in sp_ag if csv.split("_")[3]not in ['migration', 'breeding']]
                sp_nonag = [csv for csv in sp_nonag if csv.split("_")[3] not in ['migration', 'breeding']]

                merge_csv(outfolder, sp_ag, sp_nonag)
                print 'Exported table for species {0}, {1}'.format(entid, species_range_break)
            else:
                sp_ag = [csv for csv in list_ag if csv.split("_")[2] == entid]
                sp_nonag = [csv for csv in list_nonag if csv.split("_")[2] == entid]
                sp_ag = [csv for csv in sp_ag if csv.split("_")[3] == str(species_range_break)]
                sp_nonag = [csv for csv in sp_nonag if csv.split("_")[3] == str(species_range_break)]


                merge_csv(outfolder, sp_ag, sp_nonag)
                print 'Exported table for species {0}, {1}'.format(entid, species_range_break)
    else:
        merge_csv(outfolder, sp_ag, sp_nonag)
        print 'Exported table for species {0}'.format(entid)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
