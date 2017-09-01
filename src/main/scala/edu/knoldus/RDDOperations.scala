package edu.knoldus

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import java.io.{File, PrintWriter}

object RDDOperations extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  private val sparkLogger = Logger.getLogger("spark")
  val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("RDD Operations")
  val sparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  /**
    * RDD from input file
    */
  val filePath = "/home/akshay/IdeaProjects/Spark/Assignment01/src/main/resources/pagecounts-20151201-220000"
  val pagecounts = sparkSession.sparkContext.textFile(filePath)

  /**
    * Getting 10 records
    */
  val destinationFilePath = "/home/akshay/IdeaProjects/Spark/Assignment01/src/main/resources"
  new File(destinationFilePath).mkdir()
  new PrintWriter(destinationFilePath + "/pagecounts-destination.txt") {
    write(pagecounts.take(10).mkString("\n"))
    close
  }

  /**
    * Total number of records in dataset
    * Output: Total Number of records are : 7598006
    */
  sparkLogger.info("Total Number of records are : " + pagecounts.count())

  /**
    * Deriving​ ​ an​ ​ RDD​ ​ containing only​ ​ English​ ​ pages
    */
  val englishRDD = pagecounts.filter(record => record.split(" ")(0).equals("en"))

  /**
    * Number of records for english pages
    * Output: Number of records for english pages are : 2278417
    */
  sparkLogger.info("Number of records for english pages are : " + englishRDD.count())

  /**
    * ​ Pages​ ​ that​ ​ were​ ​ requested​ ​ more​ ​ than​ ​ 200,000​ ​ times​ ​ in​ ​ total
    * Output: Pages that were requested 200,000 times in total are :
              (de,1040678)
              (ru,477729)
              (Special:HideBanners,1362459)
              (pt,320239)
              (Main_Page,450191)
              (ja,335982)
              (en,4925925)
              (it,746508)
              (pl,223976)
              (fr,783966)
              (es,1083799)
    */
  val pagesResult = pagecounts.map(record => (record.split(" ")(1), record.split(" ")(2).toInt)).reduceByKey(_ + _).filter(_._2 > 200000)
  sparkLogger.info("Pages that were requested 200,000 times in total are : \n" + pagesResult.collect().mkString("\n"))

}
