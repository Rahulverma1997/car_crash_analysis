import sys
import os

from create_objects import CreateObject
import get_all_variables as gav
from car_crash_run_data_ingest import DataIngest
from validations import Validation
from car_crash_run_preprocessing import DataPreprocessing
from car_crash_run_data_transform import DataTransform
from car_crash_run_data_extraction import DataExtraction


class Execution:
    def main(self):
        try:
            ### Get Spark Object

            go = CreateObject()
            spark = go.get_spark_object(gav.envn, gav.appName)
            # Validate Spark Object
            v = Validation()
            v.get_curr_date(spark)

            ### Initiate presc_run_data_ingest Script
            # Load the City File

            print("Data ingestion started .........................")
            di = DataIngest()
            d = {}
            for file in os.listdir(gav.staging_data):
                print("File is " + file)
                file_dir = gav.staging_data + '/' + file
                print("File path is " + file_dir)
                if file.split('.')[1] == 'csv':
                    file_format = 'csv'
                    df = file.split('.')[0]
                    header = gav.header
                    inferSchema = gav.inferSchema
                elif file.split('.')[1] == 'parquet':
                    file_format = 'parquet'
                    header = 'NA'
                    inferSchema = 'NA'

                d[df] = di.load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                                      inferSchema=inferSchema)
            Primary_Person_use = d['Primary_Person_use']
            Units_use = d['Units_use']
            Charges_use = d['Charges_use']
            Damages_use = d['Damages_use']
            Endorse_use = d['Endorse_use']
            Restrict_use = d['Restrict_use']

            print("Data Ingestion Completed!!!!!!!!!!!!")

            ## Data Clean Operations ----------
            print("Data Cleaning Started ............................\n")
            dp = DataPreprocessing()
            primary_Person_use, Units_use, Damages_use, Charges_use = dp.perform_data_clean(Primary_Person_use,
                                                                                            Units_use, Damages_use,
                                                                                            Charges_use)
            print("Data Cleaning Completed .............................\n")

            # Data Transform Operations ---------------------
            print("Data transform started !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            dt = DataTransform()

            # Analysis 1
            killed_males = dt.no_of_males_killed_in_crash(primary_Person_use)
            print("no of crashes in which male are killed = " + str(killed_males))

            # Analysis 2
            two_wheeler_count = dt.two_wheelers_involved(Units_use)
            print("two wheeler count booked for crashes = " + str(two_wheeler_count))

            # Analysis 3
            state = dt.state_highest_no_accident_involved_females(primary_Person_use)
            print("state which has highest number of accident in which females are involved is " + state)

            # Analysis 4
            top_5_15_veh_make_ids = dt.top_5_15_veh_make_ids_for_injuries(Units_use)
            top_5_15_veh_make_ids.show()

            # Analysis 5
            top_group = dt.top_ethnic_group_by_body_style(primary_Person_use, Units_use)
            top_group.show()

            # Analysis 6
            top_5_zip_code = dt.top_5_zip_code_by_alcohol(primary_Person_use, Units_use)
            top_5_zip_code.show(5)

            # Analysis 7
            no_damage_distinct_id = dt.distinct_crashid_no_damage_property(Units_use, Damages_use)
            print("Analysis 7 "+ str(no_damage_distinct_id))

            # Analysis 8
            top_5_veh_make_ID = dt.top_5_veh_make(primary_Person_use, Units_use, Charges_use)
            top_5_veh_make_ID.show()

            print("Data Transformation completed !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

            #data Extraction
            de = DataExtraction()

            out_put_folder = gav.output_data + '/top_5_15_veh_make_ids'
            de.extract_files(top_5_15_veh_make_ids, 'parquet', out_put_folder, False)

            out_put_folder = gav.output_data + '/top_group'
            de.extract_files(top_group, 'parquet', out_put_folder, False)

            out_put_folder = gav.output_data + '/top_5_zip_code'
            de.extract_files(top_5_zip_code, 'parquet', out_put_folder, False)

            out_put_folder = gav.output_data + '/top_5_veh_make_ID'
            de.extract_files(top_5_veh_make_ID, 'parquet', out_put_folder, False)

            print("Data extraction test")
            print("run_car_crash_main.py is Completed. \n\n")


        except Exception as exp:
            print("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
                  "and fix it." + str(exp))
            sys.exit(1)


if __name__ == "__main__":
    print("run_car_crash_main is Started ...\n\n")
    Execution().main()
