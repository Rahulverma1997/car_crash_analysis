
################################################################
# Developed By:                                                #
# Developed Date:                                              # 
# Script Name:                                                 #
# PURPOSE: Master Script to run the entire project end to end. #
################################################################

PROJ_FOLDER="/home/${USER}/car_crash_analysis_class_format/src/main/python"

printf "Calling delete_output_paths.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"
${PROJ_FOLDER}/bin/delete_output_paths.ksh
printf "Executing delete_output_paths.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"


printf "Calling run_car_crash_main.py at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
spark-submit --master local[1] ${PROJ_FOLDER}/bin/run_car_crash_main.py
printf "Executing run_car_crash_main.py is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"
