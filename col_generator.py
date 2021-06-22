import pandas
sheet=pandas.read_csv("C:\\Users\\1803489\\Downloads\\NC_ADDRESSES.csv")
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
    #print(rows['Target_Column_Name'],rows['Target_Data_Type'],i)
    S1=''
    if(rows['Target_Data_Type']=='INT64'):
        S1='-9999'
    elif(rows['Target_Data_Type']=='STRING'):
        S1='\"###\"'
    elif(rows['Target_Data_Type']=='DATE'):
        S1='CURRENT_DATE()'
    elif(rows['Target_Data_Type']=='FLOAT64'):
        S1='-9999.99'
    elif(rows['Target_Data_Type']=='TIMESTAMP'):
        S1='CURRENT_TIMESTAMP()'
    else:
        S1=' '
    #print(S1)  
    S="IFNULL(SRC."+str(rows['Target_Column_Name'])+" ,"+S1
    S2=") <> IFNULL(TGT."+str(rows['Target_Column_Name'])+" ,"+S1+")"
    P="IFNULL(SRC_T."+str(rows['Target_Column_Name'])+" ,"+S1
    P2=") <> IFNULL(TGT_T."+str(rows['Target_Column_Name'])+" ,"+S1+")"
    if((str(rows['Target_Column_Name'])!='nan') and (rows['Target_Column_Name'] not in exclude_cols)):
        if(i!=n-8):
            l.append(S+S2+' OR')
        elif(i==n-8):
            l.append(S+S2)
    if((str(rows['Target_Column_Name'])!='nan') and (rows['Target_Column_Name'] not in exclude_cols)):
        if(i!=n-8):
            l1.append(P+P2+' OR')
        elif(i==n-8):
            l1.append(P+P2)

with open("C:\\Users\\1803489\\Downloads\\NC_ADDRESS.txt", "w") as outfile:
    outfile.write("\n".join(l))
with open("C:\\Users\\1803489\\Downloads\\NC_ADDRESS_tgt.txt", "w") as outfile:
    outfile.write("\n".join(l1))
