class DataIngest:

    def load_files(self, spark, file_dir, file_format, header, inferSchema):

        try:
            print("load_files() is Started ...")
            if file_format == 'parquet':
                df = spark. \
                    read. \
                    format(file_format). \
                    load(file_dir)
            elif file_format == 'csv':
                df = spark. \
                    read. \
                    format(file_format). \
                    options(header=header). \
                    options(inferSchema=inferSchema). \
                    load(file_dir)
        except Exception as exp:
            print("Error in the method - load_files(). Please check the Stack Trace. " + str(exp))
            raise
        else:
            print(
                f"The input File {file_dir} is loaded to the data frame. The load_files() Function is completed !!! \n\n")
        return df
