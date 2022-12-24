from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
if __name__ == '__main__':
    SparkSession1 = SparkSession \
        .builder \
        .appName("Reading csv-> write to CSV") \
        .getOrCreate()

data_file = 'sparktest.csv'
Df = SparkSession1.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(Df.count()))
Df.show()

A1 = Df.groupBy('Q1').count()
print(A1.show())


Df.createOrReplaceTempView("people")
#df2 = Df.filter(Df.Age > 57)
#df3 = SparkSession1.sql("select MOH,Age,Religion,ethnicity,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13 from people")

df2 = Df.groupBy("MOH","Age").count()

print(df2.show())

df3 = Df.groupBy("Q1" ).count()

print(df3.show())

pivotDF= Df.groupBy("MOH").pivot("Age").count("Q1")
pivotDF.show()


#df2.write.option("header",True) .csv("abcd.csv")

SparkSession1.catalog.dropTempView("people")
