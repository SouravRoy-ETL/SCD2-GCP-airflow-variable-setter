# SCD2-GCP-airflow-variable-setter
> Creating automated YML to set Variables in Apache Airflow (Composer) to reduce manual effort to 0. This activity has reduced resource focusing on Manual Tasks by 99%. 
Fault tolerance is 1% in case of wrong input passed by triggers. \n 1) Variable setter YML Structure for Landing GCP to Submission GCP \n2) Variable setter YML Sturcture for Submission GCP to DataLake GCP

# Structure to be created:

1) SCD2 Structure - As per Requirements
 
# Installing
This Framework was created on **Python 3.8.5** and uses some external libraries listed below:

### a) YML, CSV
### b) Pandas
### c) Numpy

# Build/Run Command
Use following commands to build/Run the project from the project root. 
This script accepts 3 inputs and generates 2 YML Files
### Mapping Sheet (Excel File which has the Table_Name and Columns in rows)
### Sheet Name of the Above Excel sheet
### Config File which contains (bq.Table, bq.dataset, etc)
````
python .\Run_YML_Creater.py "Mapping_Sheet" "Excel_sheet_name" "Config_file_name"
````

### Authors
* Sourav Roy (souravroy7864@gmail.com)
