package org.apache.spark.template

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.template.util.CustomLogging
import java.util.Properties
/**
  * IntelliJ IDEA template for Apache Spark Standalone App with Scala and Maven / SBT.
  */
object HealthTracker {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-template")
      .getOrCreate()
    println("hello")
    spark.sparkContext.addFile("https://data.cdc.gov/api/views/cjae-szjv/rows.json")
  //  # sc.addFile(url)
  //# sqlContext = SQLContext(sc)
 // # df = sqlContext.read.csv(SparkFiles.get("adult.csv"), header=True, inferSchema= True)
 //  df = spark.read.json(SparkFiles.get("rows.json"), header=True, inferSchema= True)
//val df = spark.read.json("src/main/resources/rows.json")
//  df.select($"name",explode($"columns"),explode($"top"))
//  array of array then use flatten($"columns")
    val healthTrackDf = spark.read.json(SparkFiles.get("rows.json"))

    //removes incomplete rows
    val filteredHealthTrackDf = healthTrackDf.na.drop()

    //fills Integer blank/null with 0
    val healthTrackfFinalDf = filteredHealthTrackDf.na.fill(0).na.fill("")


//Check that the JDBC driver is available

    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // Create the JDBC URL

    val jdbcHostname = "<hostname>"
    val jdbcPort = 1433
    val jdbcDatabase = "<database>"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    //Check connectivity to the SQLServer database

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)

    //write dataframe to jdbc database

    healthTrackfFinalDf.write.mode(SaveMode.Append).jdbc(jdbcUrl, "HealthTrack", connectionProperties)
    spark.stop()

  }

}
