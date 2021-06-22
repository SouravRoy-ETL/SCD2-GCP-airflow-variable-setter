#############################################################################################################################################
########### RAW TO CURATION FRAMEWORK - REVISED FOR SCD2 (EFFORT - 16 Hours) Comm - (MERGE_CONDITION, JOIN_CONDITION, ID Needs to automated)
#############################################################################################################################################
"""
(MERGE_CONDITION, JOIN_CONDITION, ID Needs to automated)
"""
import pandas as pd
import numpy as np
import sys
import os
print("[Start]: Start Raw To Curartion Pipeline")
sheet = pd.read_csv('input/Netcracker_D1S3_Mapping_sheetV0.4_31032021'+'.csv',encoding='utf8')#Change_To_Dynamic
l=[]
l1 = []
exclude_cols=['START_OF_VALIDITY','END_OF_VALIDITY','ACTIVE_FL','TR_FILE_ID','TR_INSERT_DATETIME','TR_INSERT_ID','TR_UPDATE_DATETIME',
 'TR_UPDATE_ID']
i=0
n=len(sheet)
#print(n)
for index,rows in sheet.iterrows():
    i=i+1
    S=''
    #print(rows['TARGET_TABLE_NAME'],rows['TARGET_COLUMN_NAME'],rows['TARGET_DATA_TYPE'],i)
    S1=''
    if(rows['TARGET_DATA_TYPE']=='INT64'):
        S1='-9999'
    elif(rows['TARGET_DATA_TYPE']=='STRING'):
        S1='###'
    elif(rows['TARGET_DATA_TYPE']=='DATE'):
        S1='CURRENT_DATE()'
    elif(rows['TARGET_DATA_TYPE']=='FLOAT64'):
        S1='-9999.99'
    elif(rows['TARGET_DATA_TYPE']=='TIMESTAMP'):
        S1='CURRENT_TIMESTAMP()'
    else:
        S1=' '
    #print(S1)
    S=""+str(rows['TARGET_TABLE_NAME'])+"|IFNULL(SRC."+str(rows['TARGET_COLUMN_NAME'])+","+S1
    S2=") <> IFNULL(TGT."+str(rows['TARGET_COLUMN_NAME'])+" ,"+S1+")"
    P=""+str(rows['TARGET_TABLE_NAME'])+"|IFNULL(SRC_T."+str(rows['TARGET_COLUMN_NAME'])+" ,"+S1
    P2=") <> IFNULL(TGT_T."+str(rows['TARGET_COLUMN_NAME'])+" ,"+S1+")"
    if((str(rows['TARGET_COLUMN_NAME'])!='nan') and (rows['TARGET_COLUMN_NAME'] not in exclude_cols)):
        if(i!=n-8):
            l.append(S+S2+' OR')
        elif(i==n-8):
            l.append(S+S2)
    if((str(rows['TARGET_COLUMN_NAME'])!='nan') and (rows['TARGET_COLUMN_NAME'] not in exclude_cols)):
        if(i!=n-8):
            l1.append(P+P2+' OR')
        elif(i==n-8):
            l1.append(P+P2)
#sheet["column_comparision"] = write("\n".join(l))
#l.join(l.rsplit("TARGET_TABLE_NAME,TARGET_COLUMN_COMPARISION", l.count("TARGET_TABLE_NAME,TARGET_COLUMN_COMPARISION") - 1))
with open("input/out/SCD2_Converted_to_Row1.csv", "w",encoding='utf8') as outfile:
    outfile.seek(0) # go back to the beginning of the file
    outfile.write("TARGET_TABLE_NAME|TARGET_COLUMN_COMPARISION\n")
    outfile.write("\n".join(l))
with open("input/out/SCD2_Converted_to_Row2.csv", "w",encoding='utf8') as outfile:
    outfile.seek(0) # go back to the beginning of the file
    outfile.write("TARGET_TABLE_NAME|TARGET_COLUMN_COMPARISION_TGT\n")
    outfile.write("\n".join(l1))
################################################################################################################################
#FOR TARGET_COLUMN_COMPARISION COLUMN
################################################################################################################################
data = pd.read_csv('input/out/SCD2_Converted_to_Row1.csv', sep='|',encoding='utf8')
tAggregaterow = data.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_COMPARISION':lambda x: list(x)}).reset_index()
tAggregaterow.to_csv('input/out/SCD2_Converted.csv', index=False,encoding='utf8')
row3 = pd.read_csv('input/out/SCD2_Converted.csv',encoding='utf8')
row3.replace('', np.nan, inplace=True)
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].astype(str).str.replace(r"[\']", r"")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace('[',"")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace("'","")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace("OR, ","OR\n")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace(']',"")
row3 = row3.apply(lambda s:s.replace('(")(")','"', regex=True))
row3.to_csv("input/out/SCD2_Temp_Join_Dont_Delete.csv", index=False)
################################################################################################################################
#FOR TARGET_COLUMN_COMPARISION_TGT COLUMN
################################################################################################################################
data1 = pd.read_csv('input/out/SCD2_Converted_to_Row2.csv', sep='|',encoding='utf8')
tAggregaterow1 = data1.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_COMPARISION_TGT':lambda x: list(x)}).reset_index()
tAggregaterow1.to_csv('input/out/SCD2_Converted.csv', index=False,encoding='utf8')
row33 = pd.read_csv('input/out/SCD2_Converted.csv',encoding='utf8')
row33.replace('', np.nan, inplace=True)
row33['TARGET_COLUMN_COMPARISION_TGT'] = row33['TARGET_COLUMN_COMPARISION_TGT'].astype(str).str.replace(r"[\']", r"")
row33['TARGET_COLUMN_COMPARISION_TGT'] = row33['TARGET_COLUMN_COMPARISION_TGT'].str.replace('[',"")
row33['TARGET_COLUMN_COMPARISION_TGT'] = row33['TARGET_COLUMN_COMPARISION_TGT'].str.replace("'","")
row33['TARGET_COLUMN_COMPARISION_TGT'] = row33['TARGET_COLUMN_COMPARISION_TGT'].str.replace("OR, ","OR\n")
row33['TARGET_COLUMN_COMPARISION_TGT'] = row33['TARGET_COLUMN_COMPARISION_TGT'].str.replace(']',"")
row33 = row33.apply(lambda s:s.replace('(")(")','"', regex=True))
row33.to_csv("input/out/SCD2_Temp_Join_Dont_Delete_TGT.csv", index=False)
join = pd.merge(row3, row33,
				on='TARGET_TABLE_NAME',
				how='inner')
join.to_csv("input/out/SCD2_Temp_FINAL_COMB.csv", index=False, sep="|")
##############################################################################################################################
#GENERATE_SRC_FIELDS AND FIELDS COLUMNS AND CREATE MERGE OF TGT & FIELDS
##############################################################################################################################
sheet_fields = pd.read_csv('input/Netcracker_D1S3_Mapping_sheetV0.4_31032021'+'.csv',encoding='utf8')#Change_To_Dynamic
lis=[]
lis1 = []
i=0
n=len(sheet_fields)
for index,rows in sheet_fields.iterrows():
    i=i+1
    S=''
    S1='SRC.'+str(rows['TARGET_COLUMN_NAME'])
    S2=str(rows['TARGET_COLUMN_NAME'])
    S=""+str(rows['TARGET_TABLE_NAME'])+"|"+S1+"|"+S2
    if((str(rows['TARGET_COLUMN_NAME'])!='nan')):
        if(i!=n-8):
            lis.append(S)
        elif(i==n-8):
            lis.append(S)
with open("input/out/Fields.csv", "w",encoding='utf8') as outfile:
    outfile.seek(0) # go back to the beginning of the file
    outfile.write("TARGET_TABLE_NAME|SRC_FIELDS|FIELDS\n")
    outfile.write("\n".join(lis))
fields_input= pd.read_csv('input/out/Fields.csv',sep='|',encoding='utf8')
tAggregateRowFields = fields_input.groupby('TARGET_TABLE_NAME').agg({'SRC_FIELDS':lambda x: list(x),'FIELDS':lambda y: list(y)}).reset_index()
tAggregateRowFields.replace('', np.nan, inplace=True)
tAggregateRowFields['SRC_FIELDS'] = tAggregateRowFields['SRC_FIELDS'].astype(str).str.replace(r"[\']", r"")
tAggregateRowFields['SRC_FIELDS'] = tAggregateRowFields['SRC_FIELDS'].str.replace('[',"")
tAggregateRowFields['SRC_FIELDS'] = tAggregateRowFields['SRC_FIELDS'].str.replace("'","")
tAggregateRowFields['SRC_FIELDS'] = tAggregateRowFields['SRC_FIELDS'].str.replace(']',"")
tAggregateRowFields['SRC_FIELDS'] = tAggregateRowFields['SRC_FIELDS'].str.replace(" ","")
tAggregateRowFields['SRC_FIELDS'] = tAggregateRowFields['SRC_FIELDS'].str.replace('"',"")
tAggregateRowFields['FIELDS'] = tAggregateRowFields['FIELDS'].astype(str).str.replace(r"[\']", r"")
tAggregateRowFields['FIELDS'] = tAggregateRowFields['FIELDS'].str.replace('[',"")
tAggregateRowFields['FIELDS'] = tAggregateRowFields['FIELDS'].str.replace("'","")
tAggregateRowFields['FIELDS'] = tAggregateRowFields['FIELDS'].str.replace(']',"")
tAggregateRowFields['FIELDS'] = tAggregateRowFields['FIELDS'].str.replace(" ","")
tAggregateRowFields['FIELDS'] = tAggregateRowFields['FIELDS'].str.replace('"',"")
tAggregateRowFields.to_csv('input/out/SCD2_Fields_Aggregate.csv', sep="|", index=False,encoding='utf8')  
tgt_input= pd.read_csv('input/out/SCD2_Temp_FINAL_COMB.csv',sep='|',encoding='utf8')
fields_input_1 = pd.read_csv('input/out/SCD2_Fields_Aggregate.csv',sep='|',encoding='utf8')
join_fields = pd.merge(tgt_input, fields_input_1,
				on='TARGET_TABLE_NAME',
				how='inner')
join_fields.to_csv("input/out/SCD2_Temp_Fields_COMB_with_TGT.csv", index=False, sep="|")
##############################################################################################################################
#CREATE PIPELINE HEADER COLUMN AND MERGE WITH EXISTING MERGED FILE
##############################################################################################################################
row2 = sheet.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_NAME':lambda x: list(x)}).reset_index()
row2.replace('', np.nan, inplace=True)
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].astype(str).str.replace(r"[\']", r"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("[",":")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("]","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(",",",:")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(" ","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace('"',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:EFFECTIVE_LOAD_DATE',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:START_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:END_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:ACTIVE_FL',"")
row2.to_csv('input/out/SCD2_temp.csv', index=False,encoding='utf8',sep="|")
out1 = pd.merge(join_fields, row2,
				on='TARGET_TABLE_NAME',
				how='inner')
out1.to_csv("input/out/SCD2_Header_MERGED_Fields_Final.csv", index=False, sep="|",encoding='utf8') 
##############################################################################################################################
#GENERATE NULL_ID, MERGE_CONDITION, JOIN_CONDITION, ID AND MERGE WITH PREVIOUS OUTPUT
##############################################################################################################################
loading_strategy = pd.read_csv('input/NC Tables D1S3 Loading strategy_v1.1_2ndApril'+'.csv',encoding='utf8')#Change_To_Dynamic
loading_join = pd.merge(out1, loading_strategy,
				on='TARGET_TABLE_NAME',
				how='inner')
loading_join.to_csv("input/out/SCD2_Load_Join.csv", index=False, sep="|",encoding='utf8')
loading_join['COUNT'] = loading_join['PRIMARY KEY COLUMN'].str.count(',')
list1=[]
list2 = []
loading_join.replace('', np.nan, inplace=True)
#CHECK FOR COUNT PRIMARY KEY
for index, rows in loading_join.iterrows():
     if (rows['COUNT']==4.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY4'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==1.0):
        S1 = 'NULL AS JOINKEY1'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==0.0):
        S1 = ''
        S2 = ''
        S3 = ''
        S4 = ''
     elif(rows['COUNT']==2.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==3.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==5.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY4, NULL AS JOINKEY5'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==6.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==7.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==8.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==9.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8, NULL AS JOINKEY9'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==10.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8, NULL AS JOINKEY9, NULL AS JOINKEY10'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==11.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8, NULL AS JOINKEY9, NULL AS JOINKEY10, NULL AS JOINKEY11'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==12.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8, NULL AS JOINKEY9, NULL AS JOINKEY10, NULL AS JOINKEY11, NULL AS JOINKEY12'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==13.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8, NULL AS JOINKEY9, NULL AS JOINKEY10, NULL AS JOINKEY11, NULL AS JOINKEY12, NULL AS JOINKEY13'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     elif(rows['COUNT']==14.0):
        S1 = 'NULL AS JOINKEY1, NULL AS JOINKEY2, NULL AS JOINKEY3, NULL AS JOINKEY 4, NULL AS JOINKEY5, NULL AS JOINKEY6, NULL AS JOINKEY7, NULL AS JOINKEY8, NULL AS JOINKEY9, NULL AS JOINKEY10, NULL AS JOINKEY11, NULL AS JOINKEY12, NULL AS JOINKEY13, NULL AS JOINKEY14'
        S2 = 'SRC_T.JOINKEY1=TGT_T.'+ str(rows['PRIMARY KEY COLUMN'])
        S3 = 'SRC.'+str(rows['PRIMARY KEY COLUMN'])+"="+'TGT.'+str(rows['PRIMARY KEY COLUMN'])
        S4 = str(rows['PRIMARY KEY COLUMN'])+' AS JOINKEY'
     else:
        S1=''
        S2=''
        S3=''
        S4=''
     S=""+str(rows['TARGET_TABLE_NAME'])+"|"+S1+"|"+S2+"|"+S3+"|"+S4
     S=""+str(rows['TARGET_TABLE_NAME'])+"|"+S1+"|"+S2+"|"+S3+"|"+S4
     if((str(rows['COUNT'])!='nan')):
         if(i!=n-8):
             list1.append(S)
         elif(i==n-8):
             list1.append(S+S2)
with open("input/out/Row1.csv", "w",encoding='utf8') as outfile:
    outfile.seek(0) # go back to the beginning of the file
    outfile.write("TARGET_TABLE_NAME|NULL_ID|MERGE_CONDITION|JOIN_CONDITION|ID\n")
    outfile.write("\n".join(list1))
Joinkey_Merge = pd.read_csv('input/out/Row1.csv',sep="|",encoding='utf8')
split = loading_join['PRIMARY KEY COLUMN'].str.split(',')
#egex.sub('', url, 1)
for x in range(1,100):
    #print(x)
    Joinkey_Merge['MERGE_CONDITION'] = Joinkey_Merge['MERGE_CONDITION'].str.replace(","," AND SRC_T.JOINKEY"+str(x)+"=TGT_T.",x)
Joinkey_Merge['ID'] = Joinkey_Merge['ID'].str.replace(","," AS JOINKEY,")
Joinkey_Merge.to_csv("input/out/Row3.csv", index=False, sep="|",encoding='utf8')
Joinkey_1 = pd.read_csv("input/out/Row3.csv", sep="|",encoding='utf8')
Joinkey_2 = pd.read_csv("input/out/SCD2_Load_Join.csv", sep="|",encoding='utf8')
Joinkey_Join = pd.merge(Joinkey_1, Joinkey_2,
				on='TARGET_TABLE_NAME',
				how='inner')
take_necessary = ['TARGET_TABLE_NAME','TARGET_COLUMN_NAME','TARGET_COLUMN_COMPARISION','TARGET_COLUMN_COMPARISION_TGT', 'NULL_ID', 'MERGE_CONDITION', 'SRC_FIELDS', 'JOIN_CONDITION', 'ID', 'FIELDS']
take_necessary1 = Joinkey_Join.reindex(columns=take_necessary)
take_necessary1.to_csv("input/out/SCD2_Load_Join_FINALE.csv", index=False, sep="|",encoding='utf8')
##############################################################################################################################
#MAIN PIPELINE TO MERGE AND SPLIT OUTPUTS ALSO RENAMING
##############################################################################################################################
file_specs = pd.read_csv('input/out/D1S3_NC.csv',encoding='utf8')#Change_To_Dynamic
row4 = pd.read_csv('input/out/SCD2_Load_Join_FINALE.csv',encoding='utf8',sep="|")
out2 = pd.merge(file_specs, row4,
				on='TARGET_TABLE_NAME',
				how='inner')
#out2.to_csv("input/out/Coms_Temp_Join_Dont_Delete3.csv", index=False, sep="|",encoding='utf8')
out2.rename(columns={"TARGET_TABLE_NAME": "TARGET_TABLE_NAME", "raw_curation_pipeline_name": "pipeline_name" ,"instance_name": "instance_name","bq.dataset": "bq_DATASET"},inplace=True)
out2.rename(columns={"gcs.project": "gcs_project", "bq.project": "bq_project" ,"bq.TempStorage": "bq_TempStorage","bq.dataset (Lower)": "bq_dataset"},inplace=True)
out2.rename(columns={" runtime_args:GCS_Input_Path(Bucket Name)": "runtime_args_GCS_Input_Path_Bucket_Name", " runtime_args:GCS_Input_Path(owner)": "runtime_args_GCS_Input_Path_owner" ,"bq.TempStorage": "bq_TempStorage"," runtime_args:GCS_Input_Path(Schema)": "runtime_args_GCS_Input_Path_Schema", " runtime_args: header": "runtime_args_header","bq.table": "bq_table", "Lower Table Name" : "Lower_Table_Name",'TARGET_COLUMN_NAME': 'header', 'TARGET_COLUMN_COMPARISION': 'column_comparision', 'TARGET_COLUMN_COMPARISION_TGT': 'column_comparision_tgt', 'NULL_ID': 'null_id', 'MERGE_CONDITION': 'merge_condition', 'JOIN_CONDITION':'join_condition', 'ID': 'id', 'SRC_FIELDS' : 'src_fields', 'FIELDS':'fields'},inplace=True)
#Step 6 - Generating Raw to Curation/ Raw to Raw file path
out2["GCS_Input_Path"] = out2["runtime_args_GCS_Input_Path_Bucket_Name"]+"/"+out2["runtime_args_GCS_Input_Path_owner"]+"/"+ out2["runtime_args_GCS_Input_Path_Schema"] + "/"+ out2["file_name"] +"/"+"processing/*/"+out2["file_name"]+"*"
#Step 7 - Deleting unnecessary columns
#Common for both Raw to Raw and Raw to Curation
out2.drop('runtime_args_GCS_Input_Path_Bucket_Name', axis=1, inplace=True)
out2.drop('runtime_args_GCS_Input_Path_Schema', axis=1, inplace=True)
out2.drop('runtime_args_GCS_Input_Path_owner', axis=1, inplace=True)
out2.drop('runtime_args_header', axis=1, inplace=True)
out2.drop('bq_DATASET', axis=1, inplace=True)
out2.drop('Lower_Table_Name', axis=1, inplace=True)
# Only for Raw to Curation in case of RR - Replace line 42 - "raw_raw_pipeline_name": "pipeline_name"
# Replace with raw_curation_pipeline_name in case of raw to raw
out2.drop('raw_raw_pipeline_name', axis=1, inplace=True)
out2.to_csv("input/out/Coms_Temp_Join_Dont_Delete2.csv", index=False, sep="|",encoding='utf8')
#Split
out2 = pd.read_csv('input/out/Coms_Temp_Join_Dont_Delete2.csv', sep='|')
columnsTitles = ['TARGET_TABLE_NAME', 'pipeline_name', 'instance_name','location']
out2 = out2.reindex(columns=columnsTitles)
out2.to_csv('input/out/final/CSV_Yaml_Generated_1.csv', index=False,sep ='|',encoding='utf8')
#Split
out3 = pd.read_csv('input/out/Coms_Temp_Join_Dont_Delete2.csv', sep='|')
columnsTitles2 = ['TARGET_TABLE_NAME','GCS_Input_Path','header','column_comparision','column_comparision_tgt','null_id','merge_condition','src_fields','join_condition','id','fields','recipe','bq_table','gcs_project','bq_project','bq_TempStorage','bq_dataset','schema']
#columnsTitles2 = ['TARGET_TABLE_NAME','GCS_Input_Path','header','column_comparision_tgt','recipe','bq_table','gcs_project','bq_project','bq_TempStorage','bq_dataset','schema']
out3 = out3.reindex(columns=columnsTitles2)
out3.to_csv('input/out/final/CSV_Yaml_Generated_2.csv', index=False,sep ='|',encoding='utf8')
##############################################################################################################################
#POST PROCESSING - REMOVAL OF ALL TEMPORARY FILES (PLEASE REMOVE WHILE TRACEBACK)
##############################################################################################################################
os.remove(r'input/out/Coms_Temp_Join_Dont_Delete2.csv')
os.remove(r'input/out/Fields.csv')
os.remove(r'input/out/Row1.csv')
os.remove(r'input/out/Row3.csv')
os.remove(r'input/out/SCD2_Converted.csv')
os.remove(r'input/out/SCD2_Converted_to_Row1.csv')
os.remove(r'input/out/SCD2_Converted_to_Row2.csv')
os.remove(r'input/out/SCD2_Fields_Aggregate.csv')
os.remove(r'input/out/SCD2_Header_MERGED_Fields_Final.csv')
os.remove(r'input/out/SCD2_Load_Join.csv')
os.remove(r'input/out/SCD2_Load_Join_FINALE.csv')
os.remove(r'input/out/SCD2_temp.csv')
os.remove(r'input/out/SCD2_Temp_Fields_COMB_with_TGT.csv')
os.remove(r'input/out/SCD2_Temp_FINAL_COMB.csv')
os.remove(r'input/out/SCD2_Temp_Join_Dont_Delete.csv')
os.remove(r'input/out/SCD2_Temp_Join_Dont_Delete_TGT.csv')
##############################################################################################################################
#SCRIPT END
##############################################################################################################################