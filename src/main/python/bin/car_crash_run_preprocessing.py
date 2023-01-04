from pyspark.sql.functions import *
from pyspark.sql.types import *


class DataPreprocessing:
    def perform_data_clean(self, primary_person_use, Units_use, Damages_use, Charges_use):
        try:
            print(f"perform_data_clean() is started for primary_person_use dataframe...\n")
            primary_person_use = primary_person_use.withColumn("CRASH_ID", col("CRASH_ID").cast('int')) \
                .withColumn("TOT_INJRY_CNT", col("TOT_INJRY_CNT").cast('int')) \
                .withColumn("PRSN_NBR", col("PRSN_NBR").cast('int')) \
                .withColumn("DEATH_CNT", primary_person_use.DEATH_CNT.cast(IntegerType()))
            primary_person_use = primary_person_use.select(col("CRASH_ID"), col("DEATH_CNT"), col("PRSN_GNDR_ID"),
                                                           col("PRSN_GNDR_ID"), col("DRVR_LIC_STATE_ID"),
                                                           col("PRSN_ETHNICITY_ID"), col("PRSN_ALC_RSLT_ID"),
                                                           col("DRVR_ZIP"), col("PRSN_TYPE_ID"),
                                                           col("DRVR_LIC_TYPE_ID"))
            print(f"perform_data_clean() completed for primary_person_use dataframe...\n")

            print(f"perform_data_clean() is started for Units_use dataframe...\n")
            units_use = Units_use.withColumn("CRASH_ID", col("CRASH_ID").cast('int')) \
                .withColumn("TOT_INJRY_CNT", col("TOT_INJRY_CNT").cast('int')) \
                .withColumn("DEATH_CNT", col("DEATH_CNT").cast('int'))

            units_use = units_use.select(col("CRASH_ID"), col("UNIT_DESC_ID"), col("VEH_BODY_STYL_ID"),
                                         col("VEH_MAKE_ID"), col("TOT_INJRY_CNT"), col("DEATH_CNT"),
                                         col("VEH_BODY_STYL_ID"), col("FIN_RESP_TYPE_ID"), col("VEH_DMAG_SCL_1_ID"),
                                         col("VEH_DMAG_AREA_2_ID"), col("VEH_COLOR_ID"), col("VEH_LIC_STATE_ID"))
            print(f"perform_data_clean() completed for Units_use dataframe...\n")

            print(f"perform_data_clean() is started for Damages_use dataframe...\n")
            Damages_use = Damages_use.withColumn("CRASH_ID", col("CRASH_ID").cast('int'))
            print(f"perform_data_clean() completed for Damages_use dataframe...\n")

            print(f"perform_data_clean() is started for Charges_use dataframe...\n")
            Charges_use = Charges_use.withColumn("CRASH_ID", col("CRASH_ID").cast('int'))
            print(f"perform_data_clean() completed for Charges_use dataframe...\n")


        except Exception as exp:
            print("Error in the method. Please check the Stack Trace. " + str(exp))
            raise
        else:
            print("perform_data_clean() is completed !!! \n\n")
        return primary_person_use, units_use, Damages_use, Charges_use
