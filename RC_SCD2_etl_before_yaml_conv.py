################################################################################################
########### RAW TO CURATION FRAMEWORK
################################################################################################
#convert ETL Code from Talend To Python
#Usage - 
# on command line - python .\RC_etl_before_yaml_conv.py "Netcracker_D1S4_Mapping_sheet_With PII_v7" "D1S4_NC"
import pandas as pd
import numpy as np
import sys
import os
print("[Start]: Start Raw To Curartion Pipeline")
sheet = pd.read_csv('input/UPC D2-S2 Mapping Sheet_v1.5_ 31march'+'.csv',encoding='utf8')
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
        S1='"###"'
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
    P="IFNULL(SRC_T."+str(rows['TARGET_COLUMN_NAME'])+" ,"+S1
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
with open("input/out/SCD2_Converted_to_Row.csv", "w",encoding='utf8') as outfile:
    outfile.seek(0) # go back to the beginning of the file
    outfile.write("TARGET_TABLE_NAME|TARGET_COLUMN_COMPARISION\n")
    outfile.write("\n".join(l))
data = pd.read_csv('input/out/SCD2_Converted_to_Row.csv', sep='|',encoding='utf8')
tAggregaterow = data.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_COMPARISION':lambda x: list(x)}).reset_index()
tAggregaterow.to_csv('input/out/SCD2_Converted.csv', index=False,encoding='utf8')
row3 = pd.read_csv('input/out/SCD2_Converted.csv',encoding='utf8')
row3.replace('', np.nan, inplace=True)
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].astype(str).str.replace(r"[\']", r"")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace('[',"")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace("'","")
row3['TARGET_COLUMN_COMPARISION'] = row3['TARGET_COLUMN_COMPARISION'].str.replace("OR, ","OR\n")
row3 = row3.apply(lambda s:s.replace('(")(")','"', regex=True))
row3.to_csv("input/out/SCD2_Temp_Join_Dont_Delete.csv", index=False)
#START_ORIGINAL_CODE
row2 = sheet.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_NAME':lambda x: list(x)}).reset_index()
row2.replace('', np.nan, inplace=True)
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].astype(str).str.replace(r"[\']", r"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("[","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("]","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(",",",:")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(" ","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace('"',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_FILE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:EFFECTIVE_LOAD_DATE',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:START_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:END_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:ACTIVE_FL',"")
row2.to_csv('input/out/SCD2_temp.csv', index=False,encoding='utf8')
out1 = pd.merge(row3, row2,
				on='TARGET_TABLE_NAME',
				how='inner')
out1.to_csv("input/out/SCD2_Join_Final_File.csv", index=False, sep="|",encoding='utf8')
file_specs = pd.read_csv('input/out/D2S2_UPC.csv',encoding='utf8')
row4 = pd.read_csv('input/SCD2_Join_Final_File.csv',encoding='utf8',sep="|")
out2 = pd.merge(file_specs, row4,
				on='TARGET_TABLE_NAME',
				how='inner')
out2.rename(columns={"TARGET_TABLE_NAME": "TARGET_TABLE_NAME", "raw_curation_pipeline_name": "pipeline_name" ,"instance_name": "instance_name","bq.dataset": "bq_DATASET"},inplace=True)
out2.rename(columns={"gcs.project": "gcs_project", "bq.project": "bq_project" ,"bq.TempStorage": "bq_TempStorage","bq.dataset (Lower)": "bq_dataset"},inplace=True)
out2.rename(columns={" runtime_args:GCS_Input_Path(Bucket Name)": "runtime_args_GCS_Input_Path_Bucket_Name", " runtime_args:GCS_Input_Path(owner)": "runtime_args_GCS_Input_Path_owner" ,"bq.TempStorage": "bq_TempStorage"," runtime_args:GCS_Input_Path(Schema)": "runtime_args_GCS_Input_Path_Schema", " runtime_args: header": "runtime_args_header","bq.table": "bq_table", "Lower Table Name" : "Lower_Table_Name",'TARGET_COLUMN_NAME': 'header', 'TARGET_COLUMN_COMPARISION': 'column_comparision'},inplace=True)
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
out2.to_csv("input/Coms_Temp_Join_Dont_Delete2.csv", index=False, sep="|",encoding='utf8')
#Split
out2 = pd.read_csv('input/Coms_Temp_Join_Dont_Delete2.csv', sep='|')
columnsTitles = ['TARGET_TABLE_NAME', 'pipeline_name', 'instance_name','location']
out2 = out2.reindex(columns=columnsTitles)
out2.to_csv('input/out/CSV_Yaml_Generated_1.csv', index=False,sep ='|',encoding='utf8')
#Split
out3 = pd.read_csv('input/Coms_Temp_Join_Dont_Delete2.csv', sep='|')
columnsTitles2 = ['TARGET_TABLE_NAME','GCS_Input_Path','header','column_comparision','recipe','bq_table','gcs_project','bq_project','bq_TempStorage','bq_dataset','schema']
out3 = out3.reindex(columns=columnsTitles2)
out3.to_csv('input/out/CSV_Yaml_Generated_2.csv', index=False,sep ='|',encoding='utf8')

























"""

#pd.DataFrame(tAggregaterow.describe()).to_csv("input/out/SCD2_Convertedw.csv", index=False, sep="|",encoding='utf8')
#data2 = pd.read_csv('input/out/SCD2_Convertedw.csv', sep='|',encoding='utf8')

tAggregaterow1 = row1.groupby('TARGET_TABLE_NAME').agg({'TARGET_COLUMN_NAME':lambda x: list(x)}).reset_index()
tAggregaterow1.to_csv('input/'+sys.argv[1]+'_temp.csv', index=False,encoding='utf8')
#Step 2 - TMAP - Remove the specific single quotes and replace "," with ",:" from TARGET_COLUMN_NAME
row2 = pd.read_csv('input/'+sys.argv[1]+'_temp.csv',encoding='utf8')
row2.replace('', np.nan, inplace=True)
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(r"[\']", r"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("[","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace("]","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(",",",:")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(" ","")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace('"',"")
#Step 3 - TMAP - Remove the specific column names from TARGET_COLUMN_NAME
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_FILE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:EFFECTIVE_LOAD_DATE',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_INSERT_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_DATETIME',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:TR_UPDATE_ID',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:START_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:END_OF_VALIDITY',"")
row2['TARGET_COLUMN_NAME'] = row2['TARGET_COLUMN_NAME'].str.replace(',:ACTIVE_FL',"")
row2.to_csv("input/Coms_Temp_Join_Dont_Delete.csv", index=False)
#Step 4 - TMAP - Inner Join the Mapping Sheet with the YML Specifications Sheet
row3 = pd.read_csv('input/'+sys.argv[2]+'.csv',encoding='utf8')
row4 = pd.read_csv('input/Coms_Temp_Join_Dont_Delete.csv',encoding='utf8')
out1 = pd.merge(row3, row4,
				on='TARGET_TABLE_NAME',
				how='inner')
#Step 5 - Renaming the Columns according to requirement
out1.rename(columns={"TARGET_TABLE_NAME": "TARGET_TABLE_NAME", "raw_curation_pipeline_name": "pipeline_name" ,"instance_name": "instance_name","bq.dataset": "bq_DATASET"},inplace=True)
out1.rename(columns={"gcs.project": "gcs_project", "bq.project": "bq_project" ,"bq.TempStorage": "bq_TempStorage","bq.dataset (Lower)": "bq_dataset"},inplace=True)
out1.rename(columns={" runtime_args:GCS_Input_Path(Bucket Name)": "runtime_args_GCS_Input_Path_Bucket_Name", " runtime_args:GCS_Input_Path(owner)": "runtime_args_GCS_Input_Path_owner" ,"bq.TempStorage": "bq_TempStorage"," runtime_args:GCS_Input_Path(Schema)": "runtime_args_GCS_Input_Path_Schema", " runtime_args: header": "runtime_args_header","bq.table": "bq_table", "Lower Table Name" : "Lower_Table_Name",'TARGET_COLUMN_NAME': 'header'},inplace=True)
#Step 6 - Generating Raw to Curation/ Raw to Raw file path
out1["GCS_Input_Path"] = out1["runtime_args_GCS_Input_Path_Bucket_Name"]+"/"+out1["runtime_args_GCS_Input_Path_owner"]+"/"+ out1["runtime_args_GCS_Input_Path_Schema"] + "/"+ out1["file_name"] +"/"+"processing/*/"+out1["file_name"]+"*" 
#Step 7 - Deleting unnecessary columns
#Common for both Raw to Raw and Raw to Curation
out1.drop('runtime_args_GCS_Input_Path_Bucket_Name', axis=1, inplace=True)
out1.drop('runtime_args_GCS_Input_Path_Schema', axis=1, inplace=True)
out1.drop('runtime_args_GCS_Input_Path_owner', axis=1, inplace=True)
out1.drop('runtime_args_header', axis=1, inplace=True)
out1.drop('bq_DATASET', axis=1, inplace=True)
out1.drop('Lower_Table_Name', axis=1, inplace=True)
# Only for Raw to Curation in case of RR - Replace line 42 - "raw_raw_pipeline_name": "pipeline_name"
# Replace with raw_curation_pipeline_name in case of raw to raw
out1.drop('raw_raw_pipeline_name', axis=1, inplace=True)
out1.to_csv("input/Coms_Temp_Join_Dont_Delete1.csv", index=False, sep="|",encoding='utf8')
#Split
out2 = pd.read_csv('input/Coms_Temp_Join_Dont_Delete1.csv', sep='|')
columnsTitles = ['TARGET_TABLE_NAME', 'pipeline_name', 'instance_name','location']
out2 = out2.reindex(columns=columnsTitles)
out2.to_csv('input/out/CSV_Yaml_Generated_1.csv', index=False,sep ='|',encoding='utf8')
#Split
out3 = pd.read_csv('input/Coms_Temp_Join_Dont_Delete1.csv', sep='|')
columnsTitles2 = ['TARGET_TABLE_NAME','GCS_Input_Path','header','recipe','bq_table','gcs_project','bq_project','bq_TempStorage','bq_dataset','schema']
out3 = out3.reindex(columns=columnsTitles2)
out3.to_csv('input/out/CSV_Yaml_Generated_2.csv', index=False,sep ='|',encoding='utf8')
#Delete Temporary Files as Post Processing
os.remove(r'input/'+sys.argv[1]+'_temp.csv')
os.remove(r'input/Coms_Temp_Join_Dont_Delete.csv')
os.remove(r'input/Coms_Temp_Join_Dont_Delete1.csv')
os.system('python VF_HU_Yaml.py'+' '+'"'+sys.argv[2]+'"')"""