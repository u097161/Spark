

# Loading CSV files from DBFS into RDDs in cluster memory

cs_test = sc.textFile('/FileStore/tables/0itamgtk1506008427622/cs_test-31b97.csv')
cs_training = sc.textFile('/FileStore/tables/94yylx7v1506008660804/cs_training-d35cb.csv') 

# Loading CSV files from DBFS into RDDs in cluster memory

cs_test = sc.textFile('/FileStore/tables/0itamgtk1506008427622/cs_test-31b97.csv')
cs_training = sc.textFile('/FileStore/tables/94yylx7v1506008660804/cs_training-d35cb.csv')


# Getting the Spark SQL context and imports
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext.getOrCreate(sc.getOrCreate())



# Reading CSV to a df, infering the schema
test_df = sqlContext.read.format("com.databricks.spark.csv") \
  .option("header", "true").option("delimiter",",").option("inferschema", "true") \
  .load("/FileStore/tables/0itamgtk1506008427622/cs_test-31b97.csv")
  
train_df = sqlContext.read.format("com.databricks.spark.csv") \
  .option("header", "true").option("delimiter",",").option("inferschema", "true") \
  .load("/FileStore/tables/94yylx7v1506008660804/cs_training-d35cb.csv")


# Check schema
print('--- Test:')
test_df.printSchema()
print('--- Training:')
train_df.printSchema()

# Check data
display(test_df)
# Check data
display(train_df)
train_df.count()
test_df.count()

# drop rows with missing values
train_df = train_df.dropna() 
train_df.count()
