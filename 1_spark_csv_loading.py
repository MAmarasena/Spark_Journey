
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType,TimestampType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('mcqpapers.com').getOrCreate()

#Type 1 : Default CSV loading with header + default data type string
df = spark.read.csv("sparktest.csv",header='true')
df.printSchema()

#Type 2 : Default CSV loading with header metioned as options + default data type string
df2 = spark.read.option("header",True) .csv("sparktest.csv")
df2.printSchema()
   
#Type 3 : CSV loading while specifying specific delimiter  + default data type string
df3 = spark.read.options(header='True', delimiter=',') .csv("sparktest.csv")
df3.printSchema()



#Type 4 : CSV loading while specifying specific schema and data type. 
#data type has to be speficiy at the begining using schema. Based on that data should be loaded
schema = StructType() \
      .add("MOH",StringType(),True) \
      .add("Age",IntegerType(),True) \
      .add("Religion",IntegerType(),True) \
      .add("ethnicity",IntegerType(),True) \
      .add("Q1",IntegerType(),True) \
      .add("Q2",IntegerType(),True) \
      .add("Q3",IntegerType(),True) \
      .add("Q4",IntegerType(),True) \
      .add("Q5",IntegerType(),True) \
      .add("Q6",IntegerType(),True) \
      .add("Q7",IntegerType(),True) \
      .add("Q8",IntegerType(),True) \
      .add("Q9",IntegerType(),True) \
      .add("Q10",IntegerType(),True) \
      .add("Q11",IntegerType(),True) \
      .add("Q12",IntegerType(),True) \
      .add("Q13",IntegerType(),True) \

df_with_schema = spark.read.format("csv") .option("header", True).schema(schema).load("sparktest.csv")
df_with_schema.printSchema()

df_with_schema.show()

print('Sliced row count:',df_with_schema.count())
print('Sliced column count:',len(df_with_schema.columns))

