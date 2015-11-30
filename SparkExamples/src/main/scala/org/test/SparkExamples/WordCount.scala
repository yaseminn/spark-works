package org.test.SparkExamples

/**
 * @author yasemin
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) = {
     if (args.length < 2) {
      println("usage: <input> <output>")
      System.exit(0)
    }
    
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    
    val text = sc.textFile(input)
    text.flatMap { line =>
      line.split(" ")
    }
      .map { word =>
        (word, 1)
      }
    .reduceByKey(_+_)
      .saveAsTextFile(output)
    

  }
}