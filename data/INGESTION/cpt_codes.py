from pyspark.sql import SparkSession,functions as f

#create spark session 
spark = SparkSession.builder.appName("CPT codes Ingestion").getOrCreate()

# configure variables
BUCKET_NAME = "healthcare-bucket-1648"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "gcp-new-1628.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

cptcodes_df= spark.read.csv(CPT_BUCKET_PATH,header=True)

#replace all space with the undesoure in the column headers
for col in  cptcodes_df.columns:
    new_col= col.replace(" ", "_").lower()
    cptcodes_df= cptcodes_df.withColumnRenamed(col,new_col)

# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())

    




