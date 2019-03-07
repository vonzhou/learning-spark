/**
  * Illustrates flatMap + countByValue for wordcount.
  */
package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark._
// 包含了RDD隐式转换
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount")

    val sc = new SparkContext(conf)

    // Load our input data.
    val input = sc.textFile(inputFile)

    // Split up into words.
    val words = input.flatMap(line => line.split(" "))

    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }


    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}
