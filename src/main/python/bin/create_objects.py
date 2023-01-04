from pyspark.sql import SparkSession


class CreateObject:
    def get_spark_object(self, envn, appName):
        try:
            print(f"get_spark_object() is started. The '{envn}' envn is used.")
            if envn == 'TEST':
                master = 'local'
            else:
                master = 'yarn'
            spark = SparkSession \
                .builder \
                .master(master) \
                .appName(appName) \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")
        except NameError as exp:
            print("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp))
            raise
        except Exception as exp:
            print("Error in the method - get_spark_object(). Please check the Stack Trace. " + str(exp))
        else:
            print("Spark Object is created ...\n\n")
        return spark
