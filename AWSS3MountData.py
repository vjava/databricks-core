# Databricks notebook source
access_key = 'access_key'
secret_key = 'secret_key'
#encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "aws_bucket_name"
mount_name = "s3dataread"

dbutils.fs.mount(f"s3a://{access_key}:{secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

file_location = "dbfs:/mnt/s3dataread/data/customers_1.csv"
#Example#
#file_location = "dbfs:/mnt/s3dataread/employee/*"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.csv(file_location, inferSchema = True, header= True)
#df.printSchema()
df.show()

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/s3dataread")
