import pandas as pd
import datetime
import os
import decimal

input_file_location = r'C:\Users\JConno02\Documents\Projects\SAM\in_test_files'
master_input_table = r'C:\Users\JConno02\Documents\Projects\SAM\ScenarioInputs_par_20171004.csv'
outpath = r'C:\Users\JConno02\Documents\Projects\SAM'
skip_formatting_check = True

# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
out_df = pd.DataFrame(
    columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type', 'error description',
             'value'])
summary_df = pd.DataFrame(
    columns=['parameter', 'values in range', 'values outside range', 'value mixed data type', 'blanks', 'zeros',
             'negative value', 'format mask does not match', 'flag'])
outfile_yellow = outpath + os.sep + 'QA_IA_Errors_SAM_yellow_' + str(date) + '.csv'
outfile_red = outpath + os.sep + 'QA_IA_Errors_SAM_red_' + str(date) + '.csv'
outfile_other = outpath + os.sep + 'QA_IA_Errors_SAM_other_' + str(date) + '.csv'
outfile_summary = outpath + os.sep + 'QA_IA_Errors_SAM_summary_' + str(date) + '.csv'

input_df = pd.read_csv(master_input_table)
list_parameters = input_df['Parameter'].values.tolist()  # should this be set by user prompt?
merged_df = pd.DataFrame(columns=list_parameters)

input_file_dict = {}
dtype_dict = {'scenario': str, 'mukey': str, 'cokey': str, 'state': str, 'cdl': int, 'weatherID': int, 'date': str,
              'leachpot': int, 'hsg': int, 'cn_ag': int, 'cn_fallow': int, 'orgC_5': float, 'orgC_20': float,
              'orgC_50': float, 'orgC_100': float, 'bd_5': float, 'bd_20': float, 'bd_50': float, 'bd_100': float,
              'fc_5': float, 'fc_20': float, 'fc_50': float, 'fc_100': float, 'wp_5': float, 'wp_20': float,
              'wp_50': float, 'wp_100': float, 'pH_5': float, 'pH_20': float, 'pH_50': float, 'pH_100': float,
              'sand_5': float, 'sand_20': float, 'sand_50': float, 'sand_100': float, 'clay_5': float, 'clay_20': float,
              'clay_50': float, 'clay_100': float, 'kwfact': float, 'slope': float, 'slp_length': float,
              'uslels': float, 'RZmax': int, 'sfac': float, 'rainfall': float, 'anedt': float, 'plntbeg': int,
              'plntend': int, 'harvbeg': int, 'harvend': int, 'emrgbeg': int, 'emrgend': int, 'blmbeg': int,
              'blmend': int, 'matbeg': int, 'matend': int, 'cintcp': float, 'covmax': int, 'amxdr': int, 'irr_pct': int,
              'irr_type': int, 'deplallw': float, 'leachfrac': float, 'cropprac': int, 'cfact_fal': float,
              'cfact_cov': float, 'ManningsN': float, 'uslep': float}


def load_data(infile, col_check, error_df, in_file_dict, d_dict):
    c_input_df = pd.read_table(infile, sep=',')
    c_input_df.fillna(-987654321, inplace=True)

    cols = c_input_df.columns.values.tolist()
    for v in c_input_df.columns.values.tolist():
        if v in d_dict.keys():
            try:
                c_input_df[v] = c_input_df[v].map(lambda t: t).astype(d_dict[v])
            except ValueError:
                # error when loading dtypes- typically because trying to set a col as in w blanks
                c_input_df[v] = c_input_df[v].map(lambda t: t).astype(str)
                error_df = error_df.append(
                    {'input file': os.path.basename(infile), 'scenario identifier': 'na', 'parameter with error': v,
                     'error': 'column header-data type error {0}'.format(v), 'error type': 'red',
                     'error description': 'could not set dtypes to int blanks -forced object', 'value': 'na'},
                    ignore_index=True)

        else:
            c_input_df[v] = c_input_df[v].map(lambda t: t).astype(str)

        if cols.index(v) % 20 == 0:
            print 'Completed data type check on {0} of {1}'.format(cols.index(v), len(cols))

    in_file_dict[(os.path.basename(infile)).split('_')[0]] = os.path.basename(infile)
    c_cols = c_input_df.columns.tolist()
    missing_col = list(set(col_check) - set(c_cols))
    add_col = list(set(c_cols) - set(col_check))
    if len(missing_col) > 0:
        print 'File {0} has a missing column header {1}'.format(os.path.basename(infile), missing_col)
        error_df = error_df.append({'input file': os.path.basename(infile), 'scenario identifier': 'na',
                                    'parameter with error': 'column header',
                                    'error': 'missing column {0}'.format(missing_col), 'error type': 'red',
                                    'error description': 'missing column', 'value': 'na'}, ignore_index=True)

    if len(add_col) > 0:
        print 'File {0} has an additional column header {1}'.format(os.path.basename(infile), add_col)
        error_df = error_df.append({'input file': os.path.basename(infile), 'scenario identifier': 'na',
                                    'parameter with error': 'column header',
                                    'error': 'additional column {0}'.format(add_col), 'error type': 'red',
                                    'error description': 'additional column', 'value': 'na'}, ignore_index=True)
    c_input_df.replace(-987654321, '', inplace=True)

    cols = c_input_df.columns.values.tolist()
    return error_df, c_input_df, cols


def len_out(row, col, format_style):
    if col in dtype_dict.keys():
        val_in_table = (dtype_dict[col])(row[col])
    else:
        val_in_table = (row[col])

    val_in_table = ('%f' % val_in_table).rstrip('0').rstrip('.') if type(val_in_table) is float else val_in_table
    d = decimal.Decimal(val_in_table)
    d_place = d.as_tuple().exponent
    par_d = decimal.Decimal(format_style)
    par_d_place = par_d.as_tuple().exponent
    error = 'green'
    if float(val_in_table) > float(format_style):
        error = 'Format mask of {0} does not match '.format(format_style, val_in_table)
    elif d_place < par_d_place:
        error = 'Format mask of {0} does not match decimal place  '.format(format_style, val_in_table)
    return error


def dis_out(row, col, value_a, value_b, cdl_flag, group):
    sce_name = row['scenario']
    cdl_code = int(sce_name.split('cdl')[1])
    val = row[col]
    error_type = 'green'

    if group == 'discrete value alpha':
        if val == value_a or val == value_b:
            error_type = 'green'
        else:
            error_type = 'red'
    elif cdl_flag:
        try:
            if int(cdl_code) <= 110 and float(val) != 0.5:
                error_type = 'red'
            elif int(cdl_code) > 110 and float(val) != 1.0:

                error_type = 'red'
        except ValueError:
            error_type = 'red'

    elif float(val) != float(value_a):
        error_type = 'red'

    else:
        error_type = 'green'

    return error_type


def org_soil(row):
    value = "{0} - orgC_5 ,{1}- orgC_20 ,{2} - orgC_50 ,{3} - orgC_100".format(str(row['orgC_5']), str(row['orgC_20']),
                                                                               str(row['orgC_50']),
                                                                               str(row['orgC_100']))
    return value


def error_des(error_nm, min_max):
    error = error_nm + ' of {0} to {1}'.format(min_max.split(',')[0], min_max.split(',')[1])
    return error


def dis_error(row, col, value_a, value_b, cdl_flag, group):
    sce_name = row['scenario']
    cdl_code = int(sce_name.split('cdl')[1])
    val = row[col]
    error = ''

    if group == 'discrete value alpha':
        error = 'default value should be {0} or {1}'.format(value_a, value_b)

    elif cdl_flag:
        try:

            if int(cdl_code) <= 110 and float(val) != 0.5:
                error = 'default value should be {0}'.format(0.5)
                error = error + 'for the cdl classes {0}'.format(cdl_code)
            elif int(cdl_code) > 110 and float(val) != 1.0:
                error = 'default value should be {0}'.format(1.0)
                error = error + ' for the cdl classes {0}'.format(cdl_code)

        except ValueError:
            error = 'default value should be an a number'
            error = error + ' for the cdl classes {0}'.format(cdl_code)

    elif float(val) != float(value_a):

        error = 'default value should be {0}'.format(value_a)
    else:
        error = ''

    return error


def error_filter_type(group_var, group):
    error_flag = 'none'
    error_type = 'none'
    filter_type = 'none'
    if group_var == 'none':
        error_flag = 'none'
        error_type = 'none'
        filter_type = 'none'
    elif group_var == 'range':
        if 'general' and 'with' in group.split(' '):
            error_flag = 'yes'
            error_type = 'red'
            filter_type = 'none'
        elif 'general' in group.split(' '):
            error_flag = 'none'
            error_type = 'none'
            filter_type = 'none'

        elif 'equal' and 'max' in group.split(" "):
            filter_type = 'equal_to_max'
            error_flag = 'yes'
            error_type = 'red'

        elif 'equal' in group.split(" "):
            filter_type = 'equal_to_both'
            error_flag = 'yes'
            error_type = 'red'
        else:
            error_flag = 'yes'
            error_type = 'red'
            filter_type = 'none'

    elif group_var == 'general':
        if 'general' not in group.split(' '):
            error_flag = 'none'
            error_type = 'none'
            filter_type = 'none'
        elif 'general' and 'equal' in group.split(' '):
            error_flag = 'yes'
            error_type = 'yellow'
            filter_type = 'equal_to_both'
        elif 'general' in group.split(' '):
            error_flag = 'yes'
            error_type = 'yellow'
            filter_type = 'none'
        else:
            error_flag = 'none'
            error_type = 'none'
            filter_type = 'none'

    elif group_var.startswith('discrete'):
        error_flag = 'yes'
        error_type = 'red'
        filter_type = 'none'

    return error_flag, filter_type, error_type


def filter_alpha(row, col):
    val = row[col]
    try:
        bool_check = any(c.isalpha() for c in val)
    except TypeError:
        bool_check = False
    return bool_check


def filter_for_range(filter_type, m_df, col, range_lst):
    m_df['bool'] = m_df.apply(lambda row: filter_alpha(row, col), axis=1)
    alpha = m_df.loc[(m_df['bool'] == True)].copy()
    num_df = m_df.loc[(m_df['bool'] == False)].copy()
    num_df = num_df.loc[(num_df[col] != -987654321)].copy()

    data_type = dtype_dict[col]
    if data_type != float:
        num_df[col] = num_df[col].map(lambda t: t).astype(str)
        num_df[col] = num_df[col].map(lambda t: t.split('.')[0]).astype(str)
        num_df[col] = num_df[col].map(lambda t: t).astype(dtype_dict[col])
    else:
        num_df[col] = num_df[col].map(lambda t: t).astype(dtype_dict[col])

    if filter_type == 'equal_to_max':
        outside_range = num_df.loc[
            ~(num_df[col] > float((range_lst.split(',')[0]))) & (num_df[col] <= float(range_lst.split(',')[1]))].copy()

        inside_range = num_df.loc[
            (num_df[col] > float((range_lst.split(',')[0]))) & (num_df[col] <= float(range_lst.split(',')[1]))].copy()

    elif filter_type == 'equal_to_both':
        outside_range = num_df.loc[
            ~(num_df[col] >= float((range_lst.split(',')[0]))) & (num_df[col] <= float(range_lst.split(',')[1]))].copy()
        inside_range = num_df.loc[
            (num_df[col] >= float((range_lst.split(',')[0]))) & (num_df[col] <= float(range_lst.split(',')[1]))].copy()
    elif filter_type == 'range greater':
        outside_range = num_df.loc[
            ~(num_df[col] > float((range_lst.split(',')[0])))].copy()
        inside_range = num_df.loc[
            (num_df[col] > float((range_lst.split(',')[0])))].copy()
    else:
        outside_range = num_df.loc[
            ~(num_df[col] > float((range_lst.split(',')[0]))) & (num_df[col] < float(range_lst.split(',')[1]))].copy()
        inside_range = m_df.loc[
            (num_df[col] > float((range_lst.split(',')[0]))) & (num_df[col] < float(range_lst.split(',')[1]))].copy()
    return outside_range, inside_range, alpha


def filter_for_discrete(m_df, col, val, val_b, group):
    if group == 'discrete value alpha':
        outside_range = m_df.loc[~(m_df[col] == val) | (m_df[col] == val_b)].copy()
        outside_range[col].replace('', 'none', inplace=True)
        outside_range = outside_range.loc[~(outside_range[col] == 'none') & (outside_range[col] == 'none')].copy()
        inside_range = m_df.loc[(m_df[col] == val) | (m_df[col] == val_b)].copy()
        alpha = pd.DataFrame()
    else:
        m_df['bool'] = m_df.apply(lambda row: filter_alpha(row, col), axis=1)
        alpha = m_df.loc[(m_df['bool'] == True)].copy()
        num_df = m_df.loc[(m_df['bool'] == False)].copy()
        num_df = num_df.loc[(num_df[col] != -987654321)].copy()
        data_type = dtype_dict[col]
        if data_type != float:
            num_df[col] = num_df[col].map(lambda t: t).astype(str)
            num_df[col] = num_df[col].map(lambda t: t.split('.')[0]).astype(str)
            num_df[col] = num_df[col].map(lambda t: t).astype(dtype_dict[col])
        else:
            num_df[col] = num_df[col].map(lambda t: t).astype(dtype_dict[col])

        if val_b != 'none':
            val = float(val)
            val_b = float(val_b)

            outside_range = num_df.loc[(num_df[col] != float(val)) | (num_df[col] != float(val_b))].copy()

            outside_range = outside_range.loc[(outside_range[col] != -987654321)].copy()
            inside_range = num_df.loc[(num_df[col] == float(val)) | (num_df[col] == float(val_b))].copy()
        else:
            val = float(val)
            outside_range = num_df.loc[~(num_df[col] == float(val))].copy()
            outside_range = outside_range.loc[(outside_range[col] != -987654321)].copy()
            inside_range = num_df.loc[(num_df[col] == float(val))].copy()
    return outside_range, inside_range, alpha


def check_zeros_blanks(par_df, col_head, m_df, error_df, st_dict, error_nm):
    list_group = list(set(par_df[col_head].values.tolist()))

    for group in list_group:
        print "  Working on group {0} : {1} of {2}".format(group, (list_group.index(group) + 1), len(list_group))
        col_df = par_df.loc[par_df[col_head] == group].copy()
        col_list = col_df['Parameter'].values.tolist()
        zero_error = pd.DataFrame(
            columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                     'error description', 'value'])

        for col in col_list:

            if col_list.index(col) % 20 == 0:
                print '    Completed data type check on col {0} of {1}'.format(col_list.index(col), len(col_list))

            working_zero_error = pd.DataFrame(
                columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                         'error description', 'value'])
            if col not in m_df.columns.values.tolist():
                pass
            else:
                m_df[col].fillna(-987654321, inplace=True)
                m_df[col].replace('', -987654321, inplace=True)

                if col_head == 'zero type':
                    filter_zero = m_df.loc[m_df[col] == 0].copy()
                else:
                    filter_zero = m_df.loc[m_df[col] == -987654321].copy()
                if len(filter_zero) == 0:
                    pass
                else:

                    filter_zero['org_soil'] = filter_zero.apply(lambda row: org_soil(row), axis=1)
                    working_zero_error['scenario identifier'] = filter_zero['scenario'].map(lambda p: str(p)).astype(
                        str)
                    working_zero_error['input file'] = filter_zero['scenario'].map(lambda p: st_dict[p[:2]]).astype(str)
                    working_zero_error.ix[:, 'parameter with error'] = col
                    if col_head == 'zero type':
                        working_zero_error['value'] = filter_zero[col].map(lambda p: p).astype(str)
                    else:
                        working_zero_error['value'] = filter_zero[col].map(lambda p: '').astype(str)
                    if group.startswith('no'):
                        working_zero_error.ix[:, 'error type'] = 'red'
                        working_zero_error.ix[:, 'parameter with error'] = col
                        working_zero_error.ix[:, 'error'] = error_nm
                        working_zero_error.ix[:, 'error description'] = error_nm
                    elif group == 'possible conditional':
                        working_zero_error.ix[:, 'error type'] = 'yellow'
                        filter_zero['error'] = error_nm + ' but possible for high orgC soil. ' + filter_zero['org_soil']
                        working_zero_error.ix[:, 'error'] = error_nm
                        working_zero_error['error description'] = filter_zero['error'].map(lambda p: str(p)).astype(str)
                    else:
                        working_zero_error.ix[:, 'error type'] = 'yellow'
                        working_zero_error.ix[:, 'parameter with error'] = col
                        working_zero_error.ix[:, 'error'] = error_nm
                        working_zero_error.ix[:, 'error description'] = error_nm
                m_df[col].replace(-987654321, '', inplace=True)

            working_zero_error = working_zero_error.drop_duplicates()
            zero_error = pd.concat([zero_error, working_zero_error])

        zero_error = zero_error.drop_duplicates()
        error_df = pd.concat([error_df, zero_error])

    return error_df


def range_limit_type(par_df, col_head, m_df, error_df, st_dict, min_col, max_col, group_var, error_nm, df_summary):
    list_group = list(set(par_df[col_head].values.tolist()))
    list_group = [group for group in list_group if group.startswith('range')]
    for group in list_group:
        error_flag, filter_type, error_type = error_filter_type(group_var, group)
        if error_flag == 'none':
            pass
        else:
            print "  Working on group {0} : {1} of {2}".format(group, (list_group.index(group) + 1), len(list_group))
            filter_par = par_df.loc[par_df[col_head] == group].copy()
            filter_par['range_values'] = filter_par[min_col] + " ," + filter_par[max_col]
            min_max_groups = list(set(list(filter_par['range_values'].values)))
            min_max_groups.remove('nan') if 'nan' in min_max_groups else None

            for min_max in min_max_groups:
                filter_min_max = filter_par.loc[filter_par['range_values'] == min_max].copy()
                col_list = filter_min_max['Parameter'].values.tolist()
                file_lst_par_error = pd.DataFrame(
                    columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                             'error description', 'value'])
                for col in col_list:

                    if col not in m_df.columns.values.tolist():
                        pass
                    else:
                        m_df[col].fillna(-987654321, inplace=True)
                        m_df[col].replace('', -987654321, inplace=True)
                        if col_list.index(col) % 20 == 0:
                            print '       Completed data type check on col {0} of {1} in range group [{4}], ' \
                                  '{2} of {3}'.format((col_list.index(col) + 1), len(col_list),
                                                      (min_max_groups.index(min_max) + 1), len(min_max_groups), min_max)
                        no_blanks = m_df.loc[(m_df[col] != -987654321)].copy()
                        blanks = m_df.loc[(m_df[col] == -987654321)].copy()
                        zeros = m_df.loc[(m_df[col] == 0)].copy()
                        neg_number = m_df.loc[(m_df[col] < 0) & (m_df[col] != -987654321)].copy()

                        if len(no_blanks) > 0:
                            outside_range, inside_range, alpha_mixed = filter_for_range(filter_type, no_blanks, col,
                                                                                        min_max)
                            outside_range = outside_range.loc[(outside_range[col] != -987654321)].copy()
                            inside_range = inside_range.loc[(inside_range[col] != -987654321)].copy()
                            outside_range['error'] = outside_range.apply(lambda row: error_des(error_nm, min_max),
                                                                         axis=1)

                            working_alpha_error = pd.DataFrame(
                                columns=['input file', 'scenario identifier', 'parameter with error', 'error',
                                         'error type', 'error description', 'value'])
                            if len(alpha_mixed) == 0:
                                pass
                            else:
                                working_alpha_error['scenario identifier'] = alpha_mixed['scenario'].map(
                                    lambda p: str(p)).astype(str)
                                working_alpha_error['input file'] = alpha_mixed['scenario'].map(
                                    lambda p: st_dict[p[:2]]).astype(str)
                                working_alpha_error.ix[:, 'parameter with error'] = col
                                working_alpha_error['value'] = alpha_mixed[col].map(lambda p: p)
                                working_alpha_error.ix[:, 'error type'] = 'red'
                                working_alpha_error.ix[:, 'error'] = 'data type'
                                working_alpha_error.ix[:, 'error description'] = 'Str in numeric column'

                            working_error = pd.DataFrame(
                                columns=['input file', 'scenario identifier', 'parameter with error', 'error',
                                         'error type', 'error description', 'value'])
                            if len(outside_range) == 0:
                                pass
                            else:
                                working_error['scenario identifier'] = outside_range['scenario'].map(
                                    lambda p: str(p)).astype(str)
                                working_error['input file'] = outside_range['scenario'].map(
                                    lambda p: st_dict[p[:2]]).astype(str)
                                working_error.ix[:, 'parameter with error'] = col
                                working_error['value'] = outside_range[col].map(lambda p: p)
                                working_error.ix[:, 'error type'] = error_type
                                working_error.ix[:, 'error'] = error_nm
                                working_error['error description'] = outside_range['error'].map(lambda p: p).astype(str)
                            working_format_error = pd.DataFrame(
                                columns=['input file', 'scenario identifier', 'parameter with error', 'error',
                                         'error type', 'error description', 'value'])
                            if not skip_formatting_check:
                                if len(inside_range) == 0:
                                    pass
                                else:
                                    if group == 'range with general' and group_var == 'general':
                                        pass
                                    else:
                                        format_style = par_df.loc[par_df['Parameter'] == col, 'Format'].iloc[0]
                                        if format_style == 'alpha':
                                            pass
                                        else:
                                            inside_range['error'] = inside_range.apply(
                                                lambda row: len_out(row, col, format_style), axis=1)
                                            inside_range_fil = inside_range.loc[
                                                (inside_range['error'] != 'green')].copy()
                                            working_format_error['scenario identifier'] = inside_range_fil[
                                                'scenario'].map(lambda p: str(p)).astype(str)
                                            working_format_error['input file'] = inside_range_fil['scenario'].map(
                                                lambda p: st_dict[p[:2]]).astype(str)
                                            working_format_error.ix[:, 'parameter with error'] = col
                                            working_format_error['value'] = inside_range_fil[col].map(
                                                lambda p: p).astype(str)
                                            working_format_error.ix[:, 'error type'] = 'yellow'
                                            working_format_error.ix[:, 'error'] = 'Format does not match mask'
                                            working_format_error['error description'] = inside_range_fil['error'].map(
                                                lambda p: p).astype(str)
                                            working_format_error = working_format_error.loc[
                                                (working_format_error['error description'] != 'green')].copy()

                                            if len(working_format_error) > 0:
                                                working_format_error = working_format_error.drop_duplicates()
                                                file_lst_par_error = pd.concat(
                                                    [file_lst_par_error, working_format_error])
                            df_summary = df_summary.append({'parameter': col, 'values in range': len(inside_range),
                                                            'values outside range': len(outside_range),
                                                            'value mixed data type': len(alpha_mixed),
                                                            'blanks': len(blanks), 'zeros': len(zeros),
                                                            'negative value': len(neg_number),
                                                            'format mask does not match': len(working_format_error),
                                                            'flag': error_type}, ignore_index=True)

                            working_error = working_error.drop_duplicates()
                            working_alpha_error = working_alpha_error.drop_duplicates()
                            working_format_error = working_format_error.drop_duplicates()
                            file_lst_par_error = pd.concat([file_lst_par_error, working_error])
                            file_lst_par_error = pd.concat([file_lst_par_error, working_alpha_error])
                            file_lst_par_error = pd.concat([file_lst_par_error, working_format_error])

                        else:
                            file_lst_par_error = file_lst_par_error.append(
                                {'input file': 'na', 'scenario identifier': 'na', 'parameter with error': col,
                                 'error': 'all columns blank {0}'.format(col), 'error type': 'red',
                                 'error description': 'no values', 'value': 'na'}, ignore_index=True)
                            df_summary = df_summary.append(
                                {'parameter': col, 'values in range': 0, 'values outside range': 0,
                                 'value mixed data type': 0, 'blanks': len(blanks), 'zeros': len(zeros),
                                 'negative value': len(neg_number), 'format mask does not match': 0,
                                 'flag': error_type}, ignore_index=True)
                        m_df[col].replace(-987654321, '', inplace=True)

                error_df = pd.concat([error_df, file_lst_par_error])

    return error_df, df_summary


def discrete_limit_type(par_df, col_head, m_df, error_df, st_dict, group_var, df_summary):
    list_group = list(set(par_df[col_head].values.tolist()))
    list_group = [group for group in list_group if group.startswith(group_var)]

    for group in list_group:
        print "  Working on group {0} : {1} of {2}".format(group, (list_group.index(group) + 1), len(list_group))
        filter_par = par_df.loc[par_df[col_head] == group].copy()
        col_list = filter_par['Parameter'].values.tolist()
        check_ag = False
        file_lst_par_error = pd.DataFrame(
            columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                     'error description', 'value'])
        for col in col_list:
            if col not in m_df.columns.values.tolist():
                pass
            else:
                m_df[col].fillna(-987654321, inplace=True)
                m_df[col].replace('', -987654321, inplace=True)

                no_blanks = m_df.loc[(m_df[col] != -987654321)].copy()
                blanks = m_df.loc[(m_df[col] == -987654321)].copy()
                zeros = m_df.loc[(m_df[col] == 0)].copy()
                neg_number = m_df.loc[(m_df[col] < 0) & (m_df[col] != -987654321)].copy()
                if col_list.index(col) % 20 == 0:
                    print "       Completed data type check on col {0} of {1}".format((col_list.index(col) + 1),
                                                                                      len(col_list))
                if col == 'cropprac':
                    check_ag = True
                if len(no_blanks) > 0:
                    value_a = filter_par.loc[filter_par['Parameter'] == col, 'discrete value a'].iloc[0]
                    value_b = filter_par.loc[filter_par['Parameter'] == col, 'discrete value b'].iloc[0]

                    outside_range, inside_range, alpha_mixed = filter_for_discrete(no_blanks, col, value_a, value_b,
                                                                                   group)
                    outside_range = outside_range.loc[(outside_range[col] != -987654321)].copy()
                    inside_range = inside_range.loc[(inside_range[col] != -987654321)].copy()

                    working_alpha_error = pd.DataFrame(
                        columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                                 'error description', 'value'])
                    if len(alpha_mixed) == 0:
                        pass
                    else:
                        working_alpha_error['scenario identifier'] = alpha_mixed['scenario'].map(lambda p: p).astype(
                            str)
                        working_alpha_error['input file'] = alpha_mixed['scenario'].map(
                            lambda p: st_dict[p[:2]]).astype(str)
                        working_alpha_error.ix[:, 'parameter with error'] = col
                        working_alpha_error['value'] = alpha_mixed[col].map(lambda p: p)
                        working_alpha_error.ix[:, 'error type'] = 'red'
                        working_alpha_error.ix[:, 'error'] = 'data type'
                        working_alpha_error.ix[:, 'error description'] = 'Str in numeric column'

                    working_error = pd.DataFrame(
                        columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                                 'error description', 'value'])

                    if len(outside_range) == 0:
                        pass

                    else:
                        outside_range['error_type'] = outside_range.apply(
                            lambda row: dis_out(row, col, value_a, value_b, check_ag, group), axis=1)
                        outside_range['error description'] = outside_range.apply(
                            lambda row: dis_error(row, col, value_a, value_b, check_ag, group), axis=1)
                        outside_range = outside_range.loc[(outside_range['error_type'] != 'green')].copy()

                        working_error['scenario identifier'] = outside_range['scenario'].map(lambda p: str(p)).astype(
                            str)
                        working_error['input file'] = outside_range['scenario'].map(lambda p: st_dict[p[:2]]).astype(
                            str)
                        working_error.ix[:, 'parameter with error'] = col
                        working_error.ix[:, 'error'] = 'Discrete value does not match mask'
                        working_error['value'] = outside_range[col].map(lambda p: p)

                        working_error['error type'] = outside_range['error_type'].map(lambda p: p)
                        working_error['error description'] = outside_range['error description'].map(lambda p: p)
                    working_format_error = pd.DataFrame(
                        columns=['input file', 'scenario identifier', 'parameter with error', 'error', 'error type',
                                 'error description', 'value'])
                    if not skip_formatting_check:
                        if len(inside_range) == 0:
                            pass
                        else:

                            format_style = par_df.loc[par_df['Parameter'] == col, 'Format'].iloc[0]
                            if format_style == 'alpha':
                                pass
                            else:
                                inside_range['error'] = inside_range.apply(lambda row: len_out(row, col, format_style),
                                                                           axis=1)
                                inside_range_fil = inside_range.loc[(inside_range['error'] != 'green')].copy()
                                working_format_error['scenario identifier'] = inside_range_fil['scenario'].map(
                                    lambda p: str(p)).astype(str)
                                working_format_error['input file'] = inside_range_fil['scenario'].map(
                                    lambda p: st_dict[p[:2]]).astype(str)
                                working_format_error.ix[:, 'parameter with error'] = col
                                working_format_error['value'] = inside_range_fil[col].map(lambda p: p).astype(str)
                                working_format_error.ix[:, 'error type'] = 'yellow'
                                working_format_error.ix[:, 'error'] = 'Format does not match mask'
                                working_format_error['error description'] = inside_range_fil['error'].map(
                                    lambda p: p).astype(str)
                                working_format_error = working_error.loc[
                                    (working_error['error description'] != 'green')].copy()
                                if len(working_format_error) > 0:
                                    working_format_error = working_format_error.drop_duplicates()
                                    file_lst_par_error = pd.concat([file_lst_par_error, working_format_error])

                    df_summary = df_summary.append({'parameter': col, 'values in range': len(inside_range),
                                                    'values outside range': len(outside_range),
                                                    'value mixed data type': len(alpha_mixed), 'blanks': len(blanks),
                                                    'zeros': len(zeros), 'negative value': len(neg_number),
                                                    'format mask does not match': len(working_format_error),
                                                    'flag': 'red'}, ignore_index=True)
                    working_error = working_error.drop_duplicates()
                    working_alpha_error = working_alpha_error.drop_duplicates()
                    working_format_error = working_format_error.drop_duplicates()
                    file_lst_par_error = pd.concat([file_lst_par_error, working_error])
                    file_lst_par_error = pd.concat([file_lst_par_error, working_alpha_error])
                    file_lst_par_error = pd.concat([file_lst_par_error, working_format_error])

                else:
                    file_lst_par_error = file_lst_par_error.append(
                        {'input file': 'na', 'scenario identifier': 'na', 'parameter with error': col,
                         'error': 'all columns blank {0}'.format(col), 'error type': 'red',
                         'error description': 'no values', 'value': 'na'}, ignore_index=True)
                    df_summary = df_summary.append(
                        {'parameter': col, 'values in range': 0, 'values outside range': 0, 'value mixed data type': 0,
                         'blanks': len(blanks), 'zeros': len(zeros), 'negative value': len(neg_number),
                         'format mask does not match': 0, 'flag': 'red'}, ignore_index=True)
                m_df[col].replace(-987654321, '', inplace=True)

        error_df = pd.concat([error_df, file_lst_par_error])
    return error_df, df_summary


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_files = os.listdir(input_file_location)
list_input = [x for x in list_files if x.endswith('.txt')]
completed_input = 0

for in_file in list_input:
    print 'Loading {0}'.format(in_file)
    if completed_input % len(list_input) == 5:
        print 'Loaded {0} of {1}'.format(completed_input, len(list_input) - 1)
    c_input = input_file_location + os.sep + in_file
    out_df, working_df, out_cols = load_data(c_input, list_parameters, out_df, input_file_dict, dtype_dict)
    merged_df = (pd.concat([merged_df, working_df], ignore_index=True))
    merged_df = merged_df.reindex(columns=out_cols)

print 'Loaded all input file\n'

print '\nChecking zero values...\n'
out_df = check_zeros_blanks(input_df, 'zero type', merged_df, out_df, input_file_dict, 'zero value')

print '\nChecking blank values...'
out_df = check_zeros_blanks(input_df, 'blank type', merged_df, out_df, input_file_dict, 'blank value')

print '\nChecking variables with set ranges...'
out_df, summary_df = range_limit_type(input_df, 'limit type', merged_df, out_df, input_file_dict, 'min', 'max', 'range',
                                      'outside set range', summary_df)

print '\nChecking variables with general ranges...'
out_df, summary_df = range_limit_type(input_df, 'limit type', merged_df, out_df, input_file_dict, 'general min',
                                      'general max', 'general', 'outside general range', summary_df)

print '\nChecking variables with discrete values...'
out_df, summary_df = discrete_limit_type(input_df, 'limit type', merged_df, out_df, input_file_dict, 'discrete',
                                         summary_df)
out_red = out_df.loc[out_df['error type'] == 'red'].copy()
out_yellow = out_df.loc[out_df['error type'] == 'yellow'].copy()

out_red.to_csv(outfile_red)
out_yellow.to_csv(outfile_yellow)

out_other = out_df.loc[(out_df['error type'] != 'yellow') | (out_df['error type'] != 'red')].copy()
out_other.to_csv(outfile_other)
summary_df.to_csv(outfile_summary)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
