# Gelato

Introduction
Please implement a solution to any number of the proposed questions below. Feel free to present additional insights on the dataset, if you find any.

# 1st part - Ingest data into relational database from JSON files
The goal of the exercise is to read a dataset in JSON format and insert the data into an RDBMS of your choice. 
The exercise can be solved using any kind of program or script of your choice that does the job. (We use Golang and/or Python to solve problems in Gelato)

The dataset you are going to use for the exercise is:
https://catalog.data.gov/dataset/air-quality-measures-on-the-national-environmental-health-tracking-network
In the website you can find useful information about the dataset. The JSON file also contains metadata describing the dataset.
Some additional details:
The solution has to be as automated as possible in all the steps, including data gathering, transformation, ingestion and result presentation.
The solution has to be prepared to deal with incomplete/wrong/missing data. (e.g. discard incomplete rows)
The database can be any open source relational database of your choice.

<h1>daily run cron job of spark submit shell script to automate the run at 1:00 AM </h1>

./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster
  --conf <key<=<value> \
  --driver-memory 2g \
  --executor-memory 20g \
  --total-executor-cores 100  \
  --jars  <comma separated dependencies>
  --class org.apache.spark.template.HealthTracker \
  /path/to/HealthTracker.jar 


  * 1 * * * /tmp/spark/spark-submit.sh
