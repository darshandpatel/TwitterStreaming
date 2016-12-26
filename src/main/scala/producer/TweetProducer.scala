package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
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
    props.put("retries", 0)
    props.put("batch.size", 16384)
    props.put("linger.ms", 1)
    props.put("buffer.memory", 33554432)

    props.put("client.id", "TweetProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)

  }

  

  def main(args: Array[String]): Unit = {

    // Read configuration through command line arguments
    val events = args(0).toInt
    val topic = args(1)
    val brokers = args(2)
    val filePath = args(3)

    // Read Twitter API config
    val twitterConfig = Source.fromFile(filePath).getLines.toList

    val producer = TweetProducer.createKafkaProducer(brokers)

  }
}
