import pandas as pd
import os
import arcpy

in_file = r'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\HUC12\UpdatedMadeFrom_Oct14_updateFromRyan\Oct14_updateFromRyan.xlsx'
outlocation = r'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\HUC12\UpdatedMadeFrom_Oct14_updateFromRyan\Grouper'
in_df = pd.read_excel(in_file, sheetname='Grouper134')


wbd_base = r'L:\NHDPlusV2'

in_gdb = r'L:\Workspace\ESA_Species\Range\HUC12\NMFS\HUC12\GDB\Fishes_HUC12.gdb'
arcpy.env.workspace = in_gdb
fc_list = arcpy.ListFeatureClasses()

list_species = in_df.columns.values.tolist()

for v in list_species:
    print "\n{0}".format(v)
    col_index = list_species.index(v)
    current_spe = in_df.iloc[:, col_index].values.tolist()
    current_spe_df = pd.DataFrame(data=current_spe, columns=["HUC_12"])
    current_spe_df.dropna(inplace=True)
    # For Salmon
    #current_spe_df['HUC_12_str'] = current_spe_df['HUC_12'].map(lambda x: '%.0f' % x)

    #For grouper with a leading 0 in HUC2
    # current_spe_df['HUC_12_str'] = current_spe_df['HUC_12'].map(lambda x: x).astype(str)
    # current_spe_df['HUC_12_str'] = current_spe_df['HUC_12_str'].map(lambda x: x.replace('.0','')).astype(str)
    # current_spe_df['HUC_12_str'] = current_spe_df['HUC_12_str'].map(lambda x: '0'+str(x)).astype(str)
    #For grouper without a leading 0 in HUC2
    current_spe_df['HUC_12_str'] = current_spe_df['HUC_12'].map(lambda x: x).astype(str)


    fc_id = 'R_' + str(v)
    fc = [i for i in fc_list if i.startswith(fc_id)]
    if len(fc) == 1:
        in_fc = in_gdb + os.sep + fc[0]
        array = arcpy.da.TableToNumPyArray(in_fc, ['HUC_12'])
        att_df = pd.DataFrame(array)

        att_df['HUC_12_str'] = att_df['HUC_12'].map(lambda x: x).astype(str)
        # NOTE this is extracting the HUC2 based on as 12-digit HUC12 number, if there is a leading 0 this will not be
        # the correct HUC because there will only be 11 digits
        att_df['HUC_2']= att_df['HUC_12'].map(lambda x: x[:2]).astype(str)

        HUC2= list(set(att_df['HUC_2'].values.tolist()))
        try:
            HUC2.remove(' ')
        except:
            pass
        if '10' in HUC2:
            HUC2.remove('10')
            HUC2.append('10U')
            HUC2.append('10L')
        if '03' in HUC2:
            HUC2.remove('03')
            HUC2.append('03N')
            HUC2.append('03W')
            HUC2.append('03S')

        removed_huc12 = att_df[att_df['HUC_12_str'].isin(current_spe_df['HUC_12_str']) == False]
        added_huc12 = current_spe_df[current_spe_df['HUC_12_str'].isin(att_df['HUC_12_str']) == False]
        print removed_huc12
        print added_huc12

        remove_list = removed_huc12 ['HUC_12_str'].values.tolist()
        add_list = added_huc12['HUC_12_str'].values.tolist()

        print "HUC12 to be added {0} and HUC12 to be removed {1}".format(len(add_list), len(remove_list))

        # export files changes to csv
        #Be aware that if the list's length is zero or one, the tuples () and (7,) will be syntax errors.
        # So this trick only works for lists of two or more members. other syntax option
        # '"HUC_12" IN (' + ','.join(map(str, oid_list)) + ')' does not include '' around the HUC12 values and leads to
        # and error
        qry_add ='"HUC_12" IN ' + str(tuple(add_list))
        qry_remove ='"HUC_12" IN ' + str(tuple(remove_list))

        arcpy.Delete_management("lyr")
        arcpy.MakeFeatureLayer_management(in_fc, "lyr")
        #print [f.name for f in arcpy.ListFields("lyr")]
        if len(remove_list)> 1:
            if not os.path.exists(outlocation+os.sep + str(fc_id)+'_removed.csv'):
                removed_huc12.to_csv(outlocation+os.sep + str(fc_id)+'_removed.csv')
            arcpy.SelectLayerByAttribute_management("lyr", "NEW_SELECTION", qry_remove)
            count = int(arcpy.GetCount_management("lyr").getOutput(0))
            if count > 0:
                arcpy.DeleteRows_management("lyr")
                print 'Deleted {0} removed HUCs'.format(count)
            else:
                pass
        elif len(remove_list) == 1:
            qry = "HUC_12" + "=" +"'"+ str((remove_list[0])+"'")
            arcpy.SelectLayerByAttribute_management("lyr", "NEW_SELECTION", qry)
            count = int(arcpy.GetCount_management("lyr").getOutput(0))
            if count > 0:
                arcpy.DeleteRows_management("lyr")
                print 'Deleted removed HUCs'
            else:
                pass

        if len(add_list)>0:
            if not os.path.exists(outlocation+os.sep + str(fc_id)+'_added.csv'):
                added_huc12.to_csv(outlocation+os.sep + str(fc_id)+'_added.csv')
            for i in HUC2:
                if len(add_list)> 1:
                    arcpy.Delete_management("lyr_add")
                    in_HUC12_shp = r'L:\NHDPlusV2\NHDPlus{0}\WBDSnapshot\WBD\WBD_Subwatershed.shp'.format(i)
                    arcpy.MakeFeatureLayer_management(in_HUC12_shp, "lyr_add")
                    arcpy.SelectLayerByAttribute_management("lyr_add", "NEW_SELECTION", qry_add)
                    count = int(arcpy.GetCount_management("lyr_add").getOutput(0))
                    if count > 0:
                        arcpy.Append_management(["lyr_add"], "lyr", "TEST","","")
                        print 'Appended values {1} for HUC {0}'.format(i, count)
                    else:
                        pass
                elif len(add_list) == 1:
                    arcpy.Delete_management("lyr_add")
                    in_HUC12_shp = r'L:\NHDPlusV2\NHDPlus{0}\WBDSnapshot\WBD\WBD_Subwatershed.shp'.format(i)
                    arcpy.MakeFeatureLayer_management(in_HUC12_shp, "lyr_add")
                    qry = "HUC_12" + "=" +"'"+ str((add_list[0])+"'")
                    arcpy.SelectLayerByAttribute_management("lyr_add", "NEW_SELECTION", qry)
                    count = int(arcpy.GetCount_management("lyr_add").getOutput(0))
                    if count > 0:
                        arcpy.Append_management(["lyr_add"], "lyr", "TEST","","")
                        print 'Appended values {1} for HUC {0}'.format(i, count)
                    else:
                        pass

    elif len(fc) == 0:
        print 'There is not a file for {0}'.format(v)
    else:
        print 'There are multiple files for species {0}'.format(v)
