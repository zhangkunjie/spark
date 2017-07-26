package com.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}
object WordCountMaster
{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("wordcount")
    conf.setMaster("spark://192.168.70.131:7077")
    conf.setJars(List("/home/hadoop/spark_jars/spark.jar"))
    val sc=new SparkContext(conf)
    val line=sc.textFile("hdfs://192.168.70.132:9000/user/hadoop/words.txt")
    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("hdfs://192.168.70.131:9000/user/hadoop/wordscount")
    sc.stop()
  }
}


