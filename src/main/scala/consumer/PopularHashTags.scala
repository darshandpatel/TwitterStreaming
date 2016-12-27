package consumer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * Created by Darshan on 12/26/16.
  */
object PopularHashTags {

  val conf = new SparkConf().setMaster("local[3]").setAppName("Spark Streaming").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)

  def main(args : Array[String]) : Unit = {

    val zookeeperHostName = args(0)
    val group = args(1)
    val topics = args(2)
    val numThreads = args(3)

    val ssc = new StreamingContext(sc, Minutes(5))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zookeeperHostName, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))

    val hashTagCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Minutes(5), 2)
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Print popular hashtags
    hashTagCounts.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular hashtags in last 10 Minutes (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s Hashtag)".format(tag, count)) }
    })

    lines.count().map(cnt => "Received " + cnt + " kafka messages.").print()

    ssc.start()
    ssc.awaitTermination()

  }
}
