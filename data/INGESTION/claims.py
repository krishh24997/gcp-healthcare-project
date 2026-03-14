from pyspark.sql import SparkSession,functions as f
from pyspark.sql.functions import input_file_name, when

#create spark session 
spark = SparkSession.builder.appName("Healthcare claims ingestion").getOrCreate()

#configure variables
BUCKET_NAME= "healthcare-bucket-1648"
CLAIMS_BUCKET_PATH=f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE="gcp-new-1628.bronze_dataset.claims"
TEMP_GCS_BUCKET=f"{BUCKET_NAME}/temp/"

#read from claims source path 
claims_df= spark.read.csv(CLAIMS_BUCKET_PATH,header=True)
#claims_df=claims_df.withColumn("file_name",input_file_name())

# adding hospital source for future reference
claims_df=claims_df.withColumn("datasource",
                                  when (input_file_name().contains("hospital2"),"hosb")
                                 .when (input_file_name().contains("hospital1"),"hosa").otherwise("none"))

# dropping dupplicates if any
claims_df = claims_df.dropDuplicates()

# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())