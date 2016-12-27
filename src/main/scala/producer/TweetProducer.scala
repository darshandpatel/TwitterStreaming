package producer

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by Darshan on 12/26/16.
  */
object TweetProducer {

  /**
    * This function creates Kafka Producer instance with the specific configuration
    * @param brokers Information related to brokers (IP addresses)
    * @return Kafka Producer instance
    */
  def createKafkaProducer(brokers : String): KafkaProducer[String, String] = {

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("bootstrap.servers", brokers)
    props.put("acks", "all")
    //props.put("retries", 0)
    //props.put("batch.size", 16384)
    //props.put("linger.ms", 10)
    //props.put("buffer.memory", 33554432)

    props.put("client.id", "TweetProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)

  }

  def buildTwitterStream(twitterConfig : List[String], queue : LinkedBlockingQueue[Status]) = {

    val consumerKey = twitterConfig(0)
    val consumerSecret = twitterConfig(1)
    val accessToken = twitterConfig(2)
    val accessTokenSecret = twitterConfig(3)

    val cb = new ConfigurationBuilder()

    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey.trim())
      .setOAuthConsumerSecret(consumerSecret.trim())
      .setOAuthAccessToken(accessToken.trim())
      .setOAuthAccessTokenSecret(accessTokenSecret.trim())

    /*
    // Search tweets
    val tf = new TwitterFactory(cb.build())
    var twitter = tf.getInstance()

    val query = new Query("merry christmas")
    query.setCount(2)
    val result = twitter.search(query)
    result.getTweets().toList.foreach{ node => println(node)}
    */

    // Build Twitter Stream
    val listener = buildStatusListener(queue)
    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
    twitterStream.addListener(listener)

    val query = new FilterQuery()
    query.track("merry christmas")
    twitterStream.filter(query)
    twitterStream
  }

  def buildStatusListener(queue : LinkedBlockingQueue[Status]) : StatusListener = {

    val listener = new StatusListener {

      override def onStatus(status:Status) {
        queue.put(status)
      }

      override def onDeletionNotice(notice : StatusDeletionNotice) {}
      override def onTrackLimitationNotice(numOfLimitStatues : Int) {}
      override def onException(ex : Exception) { ex.printStackTrace }

      override def onStallWarning(warning: StallWarning): Unit = ???

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ???
    }
    listener
  }

  def main(args: Array[String]): Unit = {

    // Read configuration through command line arguments
    val topic = args(0)
    val brokers = args(1)
    val filePath = args(2)

    val queue = new LinkedBlockingQueue[Status](1000)

    // Read Twitter API config
    val twitterConfig = Source.fromFile(filePath).getLines.toList
    val producer = TweetProducer.createKafkaProducer(brokers)
    val listener = TweetProducer.buildTwitterStream(twitterConfig, queue)
    var index : Int = 0

    while(true){

      val tweet = queue.poll()
      if(tweet == null){
        Thread.sleep(1000)
      }else{
        for(hashtag <- tweet.getHashtagEntities){
          println(hashtag.getText)
          producer.send(new ProducerRecord[String, String](topic, Integer.toString(index), hashtag.getText))
          index += 1
        }
      }
    }
  }
}
