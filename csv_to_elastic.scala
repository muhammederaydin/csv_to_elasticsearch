package com.scalarunner.csvToElastic

import org.apache.spark._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.elasticsearch.spark.sql._

object csvToElastic {

  def main(args: Array[String]) {
      // Create Spark Session With Elasticsearch Configuration
      val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Csv To Elasticsearch")
        .config("spark.es.nodes","localhost")
        .config("spark.es.port","9200")
        .getOrCreate;

      // Reade CSV File
      val df = spark.read.format("csv")
          .option("delimiter", "\t")
          .load("sample.csv")

      // Explode Product ID List Objects
      // df.withColumn("_c19", functions.explode(col("_c19")))

      // Remove \N Characters In Product_ID Column
      // df.withColumn("_c19", functions.regexp_replace(df.col("_c19"), "[\\N]", ""));

      // Create Temp View For Query
      df.createOrReplaceTempView("sample")

      // SQL Query for Spark SQL
      val query = "select * from sample"
      // val frequency_sql = "select s1._c19 as P_ID1, s2._c19 as P_ID2, count(*) as frequency from sample s1 join sample s2 on s1._c14 = s2._c14 where s1._c19 < s2._c19 and length(s1._c19) > 4 and length(s2._c19) > 4 group by s1._c19, s2._c19 order by frequency desc"

      // Execute The Query and Show
      val query_result = spark.sql(query)
      // val query_result = spark.sql(frequency_sql)
      query_result.show(10)

      // Save Query Result to Elasticsearch
      query_result.saveToEs("csvtoelastic/sampleindex")

      // Stop Session
      spark.stop()
    }
}
