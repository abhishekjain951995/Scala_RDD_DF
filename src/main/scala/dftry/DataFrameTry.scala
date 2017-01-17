package dftry

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


/**
  * Created by abhishek on 17/1/17.
  */
object DataFrameTry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DataFrameTry")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val SQLContext = new SQLContext(sc)
    val personDemographicCSVPath = "/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/dataframes/person-demo.csv"
    val personReadDF = SQLContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personDemographicCSVPath)


    val personHealthCSVPath = "/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/dataframes/person-health.csv"
    val personHealthDF = SQLContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personHealthCSVPath)

    val personInsuranceCSVPath = "/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/dataframes/person-insurance.csv"
    val personInsuranceDF = SQLContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load(personInsuranceCSVPath)

    //personReadDF.show()
    //personHealthDF.show()

    //JOIN

    //Method 1:
//    val personDF1 = personReadDF
//      .join(personHealthDF,
//        personReadDF("id") === personHealthDF("id"),
//        "left_outer"
//      )
    //Method 2 (This is preferred as heavier tables are kept on left so that they are not moved)
    val personDF = personHealthDF
      .join(broadcast(personReadDF),
        personHealthDF("id") === personReadDF("id"),
        "right_outer"
      ).drop(personHealthDF("id"))


    //personDF1.show()
    personDF.show()

    val ageLess50 = personDF.filter(personDF("age")<50)
    ageLess50.show()

    ageLess50
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/dataframes/Age")



    //**************Assignment**********************

    //Joining Insurance on right with Person + Health DF on left
    //Its a left_outer join hence rows with ids not in person will be dropped
    val personInsuranceBothDF = personDF
      .join(personInsuranceDF,
        personInsuranceDF("id")===personDF("id"),
        "left_outer"
      ).drop(personInsuranceDF("id"))


    //Filtering for valid dates
    val payerFilterAmount = personInsuranceBothDF
      .filter(to_date(personInsuranceBothDF("datevalidation"))
        .gt("2017-01-18")
      )

    //Selecting distinct (id payer amount) and grouping them by payer to find amount sum
     val payerGroupAmount = payerFilterAmount
       .select("id","payer","amount").distinct()
       .groupBy("payer")
        .sum("amount")

    payerGroupAmount
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/dataframes/PayerSum")




  }
}
