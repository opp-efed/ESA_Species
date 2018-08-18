import os
import pandas as pd
import PyPDF2
import datetime

# path_root = r'\\w2032pccth016\EFEDADMINSCANS\Science Documents'
path_root = r'V:\Science Documents\111601'
# out_path = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\DER" \
#            r"\SanFiles_content_all_20171213.csv"
out_path = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\Oxyflurofen\Other Documents\DER_111601_search_20180814.csv"


def recursive_look_within_folder(working_path, pccode):
    df_running = pd.DataFrame(columns=['pccode', 'path', 'folder', 'filename', '850 guideline', 'MRID'])
    for (path, dirs, files) in os.walk(working_path):
        if len(files) > 0:
            file_directory = [pdf for pdf in files if pdf.endswith('.pdf')]
            for c_file in file_directory:
                df = pd.DataFrame(columns=['pccode', 'path', 'folder', 'filename', '850 guideline', 'MRID'])
                path_file = path + os.sep + c_file
                try:
                    pdf_file = open(path_file, 'rb')
                    read_pdf = PyPDF2.PdfFileReader(pdf_file)
                    counter = 0
                    while counter < read_pdf.getNumPages():

                        page = read_pdf.getPage(counter)
                        page_content = page.extractText().split(' ')
                        # for v in page_content:
                        #     try:
                        #         t =datetime.datetime.strptime(v, '%m-%d-%y')
                        #         print t
                        #     except ValueError:
                        # trying to extract date formates from list of value but mm/dd/yy is not a recongized format
                        # would need to conver to mm/dd/yyyy some how, or look for numbers separated by two /


                        guide_850 = [v for v in page_content if v.startswith('850') or v.startswith('835')]
                        guide_850 = list(set(guide_850))
                        if 'MRID' in page_content:
                            index_value = page_content.index('MRID')
                            try:
                                mrid_1 = page_content[index_value + 1]
                                mrid_2 = page_content[index_value + 2]
                                if len(mrid_1) == 8 and len(mrid_2) != 8:
                                    mrid = mrid_1
                                elif len(mrid_2) == 8 and len(mrid_1) != 8:
                                    mrid = mrid_2
                                if len(mrid_1) == 9 and len(mrid_2) != 9:
                                    mrid = mrid_1
                                elif len(mrid_2) == 9 and len(mrid_1) != 9:
                                    mrid = mrid_2
                                else:
                                    mrid = 'MRID reference no 8 or 9 digit number'
                            except IndexError:
                                mrid = 'MRID reference no 8 or 9 digit number'


                        else:
                            mrid = 'No reference to MRID'
                        counter += 1


                except PyPDF2.utils.PdfReadError:
                    page_content = ['Could not read malformed PDF file']
                dir_folder = os.path.basename(path)
                df = df.append(
                    {'filename': c_file, 'pccode': '"' + str(pccode) + '"', 'path': path, 'folder': dir_folder,
                     '850 guideline': guide_850, 'MRID': mrid}, ignore_index=True)
                df_running = pd.concat([df_running, df])
    return df_running


def keywordsearch_within_folder(working_path, keywords, break_sci):
    df_running = pd.DataFrame(columns=['filename', 'keywords', 'folder', 'sci break', 'path'])

    for (path, dirs, files) in os.walk(working_path):
        if len(files) > 0:
            file_directory = [pdf for pdf in files if pdf.endswith('.pdf')]
            for c_file in file_directory:
                print c_file
                df = pd.DataFrame(columns=['filename', 'keywords', 'folder', 'sci break', 'path'])
                path_file = path + os.sep + c_file

                pdf_file = open(path_file, 'rb')
                read_pdf = PyPDF2.PdfFileReader(pdf_file)
                counter = 0
                out_keywords = []
                try:
                    while counter < read_pdf.getNumPages():
                        page = read_pdf.getPage(counter)
                        page_content = page.extractText().split(' ')
                        for i in keywords:
                            keyword = [v for v in page_content if v.startswith(i)]
                            keyword = list(set(keyword))
                            if len(keyword) > 0:
                                out_keywords.append(keyword[0])
                            else:
                                pass

                        counter += 1
                    out_keywords = list(set(out_keywords))
                    df = df.append(
                        {'filename': c_file, 'keywords': out_keywords, 'folder': path, 'sci break': break_sci,
                         'path': path + os.sep + c_file}, ignore_index=True)
                    df_running = pd.concat([df_running, df])
                except PyPDF2.utils.PdfReadError:
                    counter += 1

    return df_running


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

folder_directory = os.listdir(path_root)
# f_df = pd.DataFrame(columns=['pccode', 'path', 'folder', 'filename', '850 guideline', 'MRID'])

# for folder in folder_directory:
#     if folder_directory.index(folder) % 20 == 0:
#         print 'Completed {0} of {1} folders'.format(folder_directory.index(folder), len(folder_directory))
#         f_df.to_csv(out_path)
#     pc_code = folder
#     c_path = path_root + os.sep + folder
#     folder_df = recursive_look_within_folder(c_path, pc_code)
#     f_df = pd.concat([f_df, folder_df])
# f_df.to_csv(out_path)

f_df = pd.DataFrame(columns=['filename', 'keywords', 'folder', 'sci break', 'path'])
keywords = [u'quail', u'duck', u'fish', u'Avian', u'virginianus', u'platyrhynchos', u'mellifera', u'LC50', u'LD50',
            u'IC25', u'IC05', u'EC25', u'EC05', u'NOAEC', u'LOAEC', u'mykiss', u'macrochirus', u'promelas',
            u'variegatus', u'magna', u'bahia', u'capricornutum', u'onion', u'lettace', u'ryegrass', u'soybean',
            u'degradate', u'normal', u'reverse', u'acid', u'LC', u'LD', u'IC', u'EC', u'sediment']

for folder in folder_directory:
    if 'DER' in folder.split(" ") or 'DER' in folder.split("-") or 'DERs' in folder.split(
            " ") or 'DERs' in folder.split("-"):
        print folder
        c_path = path_root + os.sep + folder
        if 'Eco' in folder.split(" ") or 'Eco' in folder.split("-") or 'Eco' in folder.split(
                " ") or 'Eco' in folder.split("-"):
            sci_break = 'Eco'
        elif 'Fate' in folder.split(" ") or 'Fate' in folder.split("-") or 'Fate' in folder.split(
                " ") or 'DERs' in folder.split("-"):
            sci_break = 'Fate'
        else:
            sci_break = 'Other'
        folder_df = keywordsearch_within_folder(c_path, keywords, sci_break)
        print folder_df
        f_df = pd.concat([f_df, folder_df])
    else:
        pass
f_df.to_csv(out_path)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
