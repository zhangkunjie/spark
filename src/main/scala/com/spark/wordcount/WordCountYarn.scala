package com.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}
object WordCountYarn
{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("wordcount")
    conf.set("mapreduce.framework.name", "yarn")
    conf.set("yarn.resourcemanager.hostname", "192.168.70.133")
    conf.set("yarn.resourcemanager.admin.address", "192.168.70.133:8033")
    conf.set("yarn.resourcemanager.address", "192.168.70.133:8032")
    conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.70.133:8031")
    conf.set("yarn.resourcemanager.scheduler.address", "192.168.70.133:8030")
    val sc=new SparkContext(conf)
    val line=sc.textFile("hdfs://192.168.70.132:9000/user/hadoop/words.txt")
    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    sc.stop()
  }
}


