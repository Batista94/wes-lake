# Data Lake do Wes

Creation of a data lake from zero.

This datalake has been created thanks to the 'Lake of the Magician' course, taught for free on the youtube channel [Teo me Why](https://www.youtube.com/@teomewhy).

Special thanks to Téo for providing great content for free.

# About

From the data in Teo Me Why points system, we will build data ingestions in Databricks.

DB -> Raw -> Bronze -> Silver -> Silver FS -> I.A. Model

Sending the data to the S3 bucket
We've created a Python script that checks each new record (or update) that occurs in the product database. This same script sends the data from each table to S3 in .parquet format, simulating a Change Data Capture (CDC).

A full-load was carried out on 06/13/2024, for the same bucket, in a specific directory.

This script was created during some random daily lives.

Setup Databricks
On the first day of the project, we showed you how to set up the Databricks environment. That is:

- Creating the Workspace + Unity Catalog
- External Location Setup (S3 in Raw)
- Adding the data volume in Raw
- Data consumption for Bronze
- We continued with the project to carry out the first data ingestions.

We created our first notebook and read the full-load data in Raw and saved it in Bronze.

Something similar to this script:

```
df_full = (spark.read
                .format("parquet")
                .load(f"/Volumes/raw/upsell/full_load/{tablename}/"))

(df_full.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
```
