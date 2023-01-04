class DataExtraction:
    def extract_files(self, df, format, filePath, headerReq):
        try:
            print(f"Extraction - extract_files() is started...\n")
            df.write \
                .format(format) \
                .save(filePath, header=headerReq)
        except Exception as exp:
            print("Error in the method - extract(). Please check the Stack Trace. " + str(exp) + "\n")
            raise
        else:
            print("Extraction - extract_files() is completed !!! \n\n")
