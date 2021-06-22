import csv
import os
import sys
import subprocess
import shutil
###################################################################################
###python .\Run_YML_Creater.py "D2S5 NC New Mapping Sheet_8thJune" "2" "NC25_SCD2_config"
###################################################################################
os.remove(r'input/talend/Loading_Strategy_D2S3_NC.csv')
subprocess.call([r'src\fin_SCD2\fin_SCD2_run.bat'])
cmd = 'python Automated_VF_HU_RR_RC_YML_Creation.py'+' '+'"'+sys.argv[1]+'"'+' '+'"'+sys.argv[2]+'"'+' '+'"'+sys.argv[3]+'"'
os.system(cmd)