package net.mkrcah

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{DefaultPartitioner, KeyedMessage, ProducerConfig, Partitioner}
import net.mkrcah.TwitterStream.OnTweetPosted
import net.mkrcah.avro.Tweet
import twitter4j.{Status, FilterQuery}

import kafka.serializer.StringEncoder
import twitter4j.json.DataObjectFactory

object KafkaProducerApp {

  private val conf = ConfigFactory.load()

  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    props.put("serializer.class","kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    twitterStream.filter(new FilterQuery().track(Array[String]("test")).language(Array[String]("fr")))
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t:Tweet) {
    val tweetEnc = toBinary[Tweet].apply(t)
    val random = new java.util.Random
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new KeyedMessage[String, String](KafkaTopic,random.toString ,toJson(t.getSchema).apply(t))
    kafkaProducer.send(msg)
  }

}



