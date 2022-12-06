# Sleep Analysis Readme


## Directory structure

 - /ana_code - source code for analytics
 - /data_ingest - data source
 - /etl_code - cleaning code
 - /profiling_code - profiling code
 - /screenshots - screenshots showing how to run the project
 - readme.md - this readme  

## Input Data
I retrieved this input from my apple watch. I have uploaded it to google drive, with the link below. 

All steps in this readme moves forward assuming that the csv is already located in the data_ingest folder. 

The input data can be found here: https://drive.google.com/file/d/1H8A2gHweL1zftR-lHTnTdkSGR73OuuOZ/view?usp=sharing


## Step by step

First we must clean the dataset:

    cd etl_code
    hadoop jar clean.jar Clean project/input/autosleep.csv project/clean/output

Then we must profile it

    cd ..
    cd profiling_code
    hadoop jar countLines.jar CountLines project/clean/output/clean.csv project/profiling/output


Now we are ready to convert the output file into a csv to prepare for analysis.

    hdfs dfs -mv project/clean/output/part-r-00000 project/clean/output/clean.csv

Then, we can run the analysis

    spark-shell --deploy-mode client
    :load /home/dh3144/homeworks/project/ana_code/analysis.scala

It is important to ensure that the directories are correct. 
