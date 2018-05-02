import os
import pandas as pd

path_root = r'C:\Users\JConno02\Documents\GitHub\ESA_Scripts_Fall2016'
out_path = r"C:\Users\JConno02\Documents\GitHub\ESA_Scripts_Fall2016\script_tracker.csv"


def recursive_look_within_folder(working_path, ):
    df_running = pd.DataFrame(columns=['path', 'folder', 'filename'])
    for (path, dirs, files) in os.walk(working_path):
        df = pd.DataFrame(columns=['path', 'folder', 'filename'])
        if len(files) > 0:
            dir_folder = os.path.basename(path)
            file_directory = [pdf for pdf in files if pdf.endswith('.py')]
            files_series = pd.Series(file_directory)
            df['filename'] = files_series.values
            df.ix[:, 'path'] = path
            df.ix[:, 'folder'] = dir_folder
            df_running = pd.concat([df_running, df])
    return df_running


folder_directory = os.listdir(path_root)
f_df = pd.DataFrame(columns=['path', 'folder', 'filename'])
for folder in folder_directory:
    if folder_directory.index(folder) % 20 == 0:
        print 'Completed {0} of {1} folders'.format(folder_directory.index(folder), len(folder_directory))
        f_df.to_csv(out_path)
    c_path = path_root + os.sep + folder
    folder_df = recursive_look_within_folder(c_path)
    f_df = pd.concat([f_df, folder_df])
f_df.to_csv(out_path)
