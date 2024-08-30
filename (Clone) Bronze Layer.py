# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog allada_joshita_databricks_npmentorskool_onmicrosoft_com;
# MAGIC create schema if not exists bronze;
# MAGIC use schema bronze;

# COMMAND ----------

csv_files = dbutils.fs.ls(f"dbfs:/mnt/data/")
Tables = [file.name[:-4] for file in csv_files if file.name.endswith('.csv')]
print(Tables)

# COMMAND ----------

import re

def clean_and_save_csv(mount_name, table_name): 
    df = spark.read.option("header", "true").csv(f"dbfs:/mnt/{mount_name}/{table_name}.csv")
    print("Original Columns:", df.columns)
    cleaned_columns = []

    for col in df.columns:
        new_col = col.replace(" ", "_").replace(",", "_").replace(";", "").replace("{", "_") \
                     .replace("}", "").replace(")", "_").replace("(", "_").replace("\n", "_") \
                     .replace("\t", "_").replace("-", "").rstrip('_')                  
        df = df.withColumnRenamed(col, new_col) 

    print("Cleaned Columns:", df.columns)

    temp_path = f"dbfs:/mnt/{mount_name}/temp_cleaned_{table_name}"
    df.write.option("header","true").csv(temp_path, mode="overwrite")
    print(f"Cleaned data written to {temp_path}")

for table in Tables: 
    clean_and_save_csv('ecom_data', table)
 

# COMMAND ----------

#creating empty tables in the bronze layer for each csv file for table in Tables
for table in Tables:
    create_table_query =f"""
    CREATE TABLE IF NOT EXISTS {table}
    """
    spark.sql(create_table_query)

    file_path = f"dbfs:/mnt/ecom_data/temp_cleaned_{table}/"
    if dbutils.fs.ls(file_path):
        copy_into_query = f"""
        COPY INTO {table}
        FROM '{file_path}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS (
            "header" = 'true',
            "inferSchema" = 'true',
            "mergeSchema" = 'true',
            "timestampFormat" = 'dd-MM-yyyy HH.mm',
            "quote" = '"'
        )
        COPY_OPTIONS ("mergeSchema" = 'true')
        """
        spark.sql(copy_into_query)
    else:
        print(f"File {file_path} does not exist.")
 

# COMMAND ----------

for table in Tables:
        spark.sql(f"ALTER TABLE bronze.{table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")  
