from pyspark.sql.functions import *
from pyspark.sql.window import Window

class DataTransform:
    def no_of_males_killed_in_crash(self, df):
        try:
            print("Transform - no_of_males_killed_in_crash() is started...\n")
            df = df.filter((df.DEATH_CNT > 0) & (df.PRSN_GNDR_ID == "MALE")).dropDuplicates(["CRASH_ID"])
            killed_males = df.count()

        except Exception as exp:
            print("Error in the method - no_of_males_killed_in_crash(). Please check the Stack Trace. \n\n" + str(exp)+ "\n")
            raise
        else:
            print("Analysis 1 is completed !!! \n\n")
        return killed_males

    def two_wheelers_involved(self, df):

        try:
            print("Transform - two_wheelers_involved() is started...\n")
            no_two_wheelers = df.filter((col("UNIT_DESC_ID").isin(["PEDALCYCLIST"])) | col("VEH_BODY_STYL_ID") \
                                        .isin(["MOTORCYCLE", "POLICE MOTORCYCLE"]))
            two_wheeler_count = no_two_wheelers.count()

        except Exception as exp:
            print("Error in the method - two_wheelers_involved(). Please check the Stack Trace. \n\n" + str(exp)+ "\n")
            raise
        else:
            print("analysis 2 - two_wheelers_involved() is completed...\n\n")
        return two_wheeler_count

    def state_highest_no_accident_involved_females(self, df):
        try:
            print("Transform - state_highest_no_accident_involved_females() is started...\n")
            window_state = Window.partitionBy("DRVR_LIC_STATE_ID")
            df = df.filter(df.PRSN_GNDR_ID == "FEMALE").select(df.CRASH_ID, df.DRVR_LIC_STATE_ID)
            df = df.withColumn("count", count("CRASH_ID").over(window_state)).orderBy(col("count").desc())
            state = df.first()['DRVR_LIC_STATE_ID']

            # no_accident_female_involved = female_involved.count()

        except Exception as exp:
            print(
                "Error in the method - state_highest_no_accident_involved_females(). Please check the Stack Trace. \n\n" + str(
                    exp)+ "\n")
            raise
        else:
            print("analysis 3 - highest_no_accident_involved_females() is completed...\n\n")
        return state

    def top_5_15_veh_make_ids_for_injuries(self, df):
        try:
            print("Transform - top_5_15_veh_make_ids_for_injuries() is started...\n")
            window_constant = Window.partitionBy(col("constant")).orderBy(col("total_death&injuries").desc())
            df = df.select(col("CRASH_ID"), col("VEH_MAKE_ID"), col("TOT_INJRY_CNT"), col("DEATH_CNT")) \
                .withColumn("sum", col("TOT_INJRY_CNT") + col("DEATH_CNT")).filter(col("sum") > 0)
            df = df.groupBy("VEH_MAKE_ID").agg(sum("sum").alias("total_death&injuries")) \
                .withColumn("constant", lit("cons"))

            df = df.withColumn("row", row_number().over(window_constant))
            df = df.filter(col("row").between(5, 15)).drop(col("constant"))

        except Exception as exp:
            print(
                "Error in the method - top_5_15_veh_make_ids_for_injuries(). Please check the Stack Trace. \n\n" + str(exp)+ "\n")
            raise
        else:
            print("analysis 4 - top_5_15_veh_make_ids_for_injuries() is completed...\n\n")
        return df

    def top_ethnic_group_by_body_style(self, df1, df2):
        try:
            print("Transform - top_ethnic_group_by_body_style() is started...\n")
            df1 = df1.select(df1.CRASH_ID, df1.PRSN_ETHNICITY_ID)
            df2 = df2.select(col("CRASH_ID").alias("CRASH_ID_2"), col("VEH_BODY_STYL_ID"))

            window_veh = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())

            top_group = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID_2)

            top_group = top_group.drop(col("CRASH_ID")).drop(col("CRASH_ID_2")) \
                .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()
            top_group = top_group.withColumn("row", row_number().over(window_veh)).filter(col("row") == 1).drop("row")

        except Exception as exp:
            print("Error in the method - top_ethnic_group_by_body_style(). Please check the Stack Trace. \n\n" + str(
                exp) + "\n")
            raise
        else:
            print("analysis 5 - top_ethnic_group_by_body_style() is completed...\n\n")
        return top_group

    def top_5_zip_code_by_alcohol(self, df1, df2):
        try:
            print("Transform - top_5_zip_code_by_alcohol() is started...\n")
            df1 = df1.select(df1.CRASH_ID, df1.PRSN_ALC_RSLT_ID, df1.DRVR_ZIP)
            df2 = df2.select(col("CRASH_ID").alias("CRASH_ID_2"), col("VEH_BODY_STYL_ID"))

            window_zip = Window.partitionBy("DRVR_ZIP")

            df_alcohol_join = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID_2).distinct()
            df_alcohol_join = df_alcohol_join.filter(df_alcohol_join.VEH_BODY_STYL_ID.like("%CAR%")) \
                .filter(col("DRVR_ZIP").isNotNull()).filter(col("PRSN_ALC_RSLT_ID") == "Positive")

            top_5_zip_code = df_alcohol_join.withColumn("count", count("CRASH_ID").over(window_zip)) \
                .dropDuplicates(["DRVR_ZIP", "count"]).orderBy(col("count").desc()) \
                .drop(col("CRASH_ID_2")).drop(col("CRASH_ID"))

        except Exception as exp:
            print("Error in the method - top_5_zip_code_by_alcohol(). Please check the Stack Trace. \n\n" + str(
                exp) + "\n")
            raise
        else:
            print("analysis 6 - top_5_zip_code_by_alcohol() is completed...\n\n")
        return top_5_zip_code

    def distinct_crashid_no_damage_property(self, df1, df2):
        try:
            print("Transform - distinct_crashid_no_damage_property() is started...\n")
            df1 = df1.select(df1.CRASH_ID, df1.VEH_BODY_STYL_ID,
                             df1.FIN_RESP_TYPE_ID, df1.VEH_DMAG_SCL_1_ID, df1.VEH_DMAG_AREA_2_ID)

            df1 = df1.filter(col("FIN_RESP_TYPE_ID").like("%INSURANCE%")).filter(df1.VEH_BODY_STYL_ID.like("%CAR%")) \
                .filter(
                (col("VEH_DMAG_SCL_1_ID").isin(["DAMAGED 4", "DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])) | (
                    col("VEH_DMAG_AREA_2_ID").isin(["DAMAGED 4", "DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])))

            df2 = df2.select(col("CRASH_ID").alias("CRASH_ID_2"), col("DAMAGED_PROPERTY"))

            no_damage_distinct_id = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID_2, "leftanti").dropDuplicates(
                ["CRASH_ID"]).count()


        except Exception as exp:
            print("Error in the method - distinct_crashid_no_damage_property(). Please check the Stack Trace. \n\n" + str(
                exp) + "\n")
            raise
        else:
            print("analysis 7 - distinct_crashid_no_damage_property() is completed...\n\n")
        return no_damage_distinct_id

    def top_5_veh_make(self, df1, df2, df3):
        try:
            print("Transform - top_5_veh_make() is started...\n")

            # states with highest number of offences
            df3_0 = df3.select(col("CRASH_ID").alias("CRASH_ID_3"))
            df2_0 = df2.select(col("CRASH_ID").alias("CRASH_ID_2"), col("VEH_LIC_STATE_ID"), col("VEH_BODY_STYL_ID"))
            df2_1 = df2_0.filter(col("VEH_BODY_STYL_ID").like("%CAR%")) \
                .filter((~col("VEH_LIC_STATE_ID").rlike("^[0-9]*$")) | (col("VEH_LIC_STATE_ID") == "NA"))

            top_25_states = df2_1.join(df3_0, df2_1.CRASH_ID_2 == df3_0.CRASH_ID_3, "leftSemi") \
                .groupBy("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).select("VEH_LIC_STATE_ID")

            # top 10 used Vehicle colours
            df2_colour = df2.filter((~col("VEH_COLOR_ID").rlike("^[0-9]*$")) | (col("VEH_COLOR_ID") == "NA")) \
                .select(col("VEH_COLOR_ID")).groupBy(col("VEH_COLOR_ID")).count().orderBy(col("count").desc()).limit(
                10).select("VEH_COLOR_ID")

            # drivers with_speed releted offenses
            df1_2 = df1.select(col("CRASH_ID"), col("PRSN_TYPE_ID"), col("DRVR_LIC_TYPE_ID")) \
                .filter(col("PRSN_TYPE_ID").isin(["DRIVER", "DRIVER OF MOTORCYCLE TYPE VEHICLE"])) \
                .filter(col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))

            df3_1 = df3.filter(col("CHARGE").like("%SPEED%")).select(col("CRASH_ID").alias("CRASH_ID_3"))
            # ids where drivers are charged with speed related offences and had license
            df1_2 = df1_2.join(df3_1, df1_2.CRASH_ID == df3_1.CRASH_ID_3, "leftSemi").select(
                col("CRASH_ID").alias("CRASH_ID_2"))

            # final
            df2_select = df2.select(col("CRASH_ID"), col("VEH_MAKE_ID"), col("VEH_COLOR_ID"),
                                    col("VEH_LIC_STATE_ID"))

            df2_join = df2_select.join(df1_2, df2_select.CRASH_ID == df1_2.CRASH_ID_2, "leftSemi") \
                .join(df2_colour, df2_select.VEH_COLOR_ID == df2_colour.VEH_COLOR_ID, "leftSemi") \
                .join(top_25_states, df2_select.VEH_LIC_STATE_ID == top_25_states.VEH_LIC_STATE_ID, "leftSemi")

            top_5_veh_make_ID = df2_join.groupBy(col("VEH_MAKE_ID")).count().orderBy(col("count").desc()).limit(5)

        except Exception as exp:
            print("Error in the method - top_5_veh_make(). Please check the Stack Trace. \n\n" + str(exp) + "\n")
            raise
        else:
            print("analysis 8 - top_5_veh_make() is completed...\n\n")
        return top_5_veh_make_ID
