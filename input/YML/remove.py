import sys
import os
import shutil
import time
from pathlib import Path
with open('NC25_SCD2_config_1624360292.yml', 'r') as file :
  filedata = file.read()
filedata = filedata.replace('###', '\"###\"')
with open('NC25_SCD2_config_1624360292.yml', 'w') as file:
  file.write(filedata)