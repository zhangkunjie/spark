import org.apache.spark.{SparkConf, SparkContext}

object Transformations
{

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("wordcount")
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    val line=sc.textFile("hdfs://192.168.70.132:9000/user/hadoop/words.txt")
    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("hdfs://192.168.70.132:9000/user/hadoop/wordscount")
    //line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    sc.stop()

  }
}