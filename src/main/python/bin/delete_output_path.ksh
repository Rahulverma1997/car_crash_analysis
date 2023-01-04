############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Delete HDFS Output paths so that Spark 
#          extraction will be smooth.                      #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="delete_output_paths.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
PROJ_FOLDER="/home/${USER}/car_crash_analysis_class_format/src/main/python"
###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

OUTPUT_PATH = ${PROJ_FOLDER}/output
if [ -d "$OUTPUT_PATH" ]
  then
  printf "The output directory $OUTPUT_PATH is available. Proceed to delete."
  rm -r -f $OUTPUT_PATH
  printf "The Output directory $OUTPUT_PATH is deleted before extraction !!! \n\n"
fi

echo "${JOBNAME} is Completed...: $(date)"

}
