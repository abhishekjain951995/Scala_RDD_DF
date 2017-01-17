package rddtry

import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
  * First Spark Program
  * Created by abhishek on 17/1/17.
  */
object RDDTryOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    //Reading Text File (Source)
//    val resourcePath = getClass.getResource("warandpeace.txt").getFile
//    println(resourcePath)

    val ipPath = "/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/RDD/warandpeace.txt"
    val readRDD = sc.textFile(ipPath)
        .cache()


    //Displaying initial 10 lines (Action)
    val x = readRDD.take(10)
    //x.foreach(println)

    //creates RDD (Transformation)
    val sampleRDD = readRDD.sample(withReplacement = false,1,123)
    sampleRDD.take(10).foreach(println)

    //Splitting into words

    //Method 1
//    val opRDD = sampleRDD.map(x => x.toLowerCase().split(" ").toList)
//    val splitRDD = opRDD.flatMap(x=>x)
//    splitRDD.take(10).foreach(println)


    //Method 2
    val splitRDD = sampleRDD.flatMap(x => x.toLowerCase().split(" ")).cache()
    splitRDD.take(10).foreach(println)

      //creates array of string not RDD (Action)
//    val z = readRDD.takeSample(withReplacement = false,10,123)
//    z.foreach(println)


    println("TotalCount " + readRDD.count())
    println("Sample Count " + sampleRDD.count())

    //Filter Stop Words
    val stopword = List("is","of","are","a","the","an","am","at","from","","and")
    println("Total Output Word Count " + splitRDD.count())
    val filteredRDD = splitRDD.filter(x => !stopword.contains(x))
    println("Total Filtered Count " + filteredRDD.count())

    //Word Count
    val wordUnitRDD = filteredRDD.map(x => (x,1))
    val wordCountRDD = wordUnitRDD
      .groupBy(x=>x._1)
      .map(x=>{
        val key = x._1
        val totalCount = x._2.size
        (key,totalCount)
      })

    wordCountRDD.foreach(println)



    //Frequency Count

    val freqUnitRDD = wordCountRDD.map(x =>(x._2,1))
    val freqCountRDD = freqUnitRDD
      .groupBy(x => x._1)
      .map(x => {
        val key = x._1
        val freqC = x._2.size
        (key,freqC)
      })
    println("++++++++++++++++++++++++++++FREQUENCY COUNT IN ASCENDING ORDER++++++++++++++++++++++++++++++++++++++++++")
    freqCountRDD.coalesce(1).sortBy(x=>x._1).foreach(println)

    val freqFilePath = "/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/RDD/FreqCount"
    //freqCountRDD.saveAsTextFile(freqFilePath)
    //freqCountRDD.saveAsObjectFile(freqFilePath)

    freqCountRDD.map(a => a._1 + "," + a._2).saveAsTextFile(freqFilePath)



    //Top 50 Percent Data using SORT
//    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//
//    val top50RDD = wordCountRDD.coalesce(1).sortBy(x => x._2,false).take(topHalf)
//
//    top50RDD.foreach(println)

    //Top 50 Percent Data Using TOP
    println("++++++++++++++++++++++++++++TOP 50 WORDS++++++++++++++++++++++++++++++++++++++++++")
    val totalWords = wordCountRDD.count()
    val topHalf = totalWords.toInt/2
    val top50 = wordCountRDD
      .map(x=>{
      (x._2,x._1)
      })
      .top(topHalf)

    top50.foreach(println)

    val filename = "/home/abhishek/Desktop/Scala Assignment/SparkHandsOn/src/main/resources/RDD/Top50"
    val writer = new PrintWriter(new FileOutputStream(filename))
    top50.foreach(x=>writer.write(x._2+" "+x._1+"\n"))
    writer.close()
    println("\n")
    println("Total Document words are "+ splitRDD.count())
    println("Filtered Document words are "+ filteredRDD.count())
    println("Distinct Words are "+ wordCountRDD.count())
    println("Top 50 Words are "+ top50.length)

  }

}
