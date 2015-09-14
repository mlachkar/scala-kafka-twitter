package net.mkrcah

import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.twitter.bijection.avro.SpecificAvroCodecs
import net.mkrcah.avro.Tweet
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.avro.Schema
import twitter4j.{Status, FilterQuery}

object KafkaConsumerApp extends App{

  private val conf = ConfigFactory.load()

  val sparkConf = new SparkConf().setAppName("kafka-twitter-spark-example").setMaster("local[*]")
  val sc = new StreamingContext(sparkConf, Seconds(5))

  val encTweets = {
    val topics = KafkaProducerApp.KafkaTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> conf.getString("kafka.brokers"))
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder ](sc, kafkaParams, topics)
}

  val jsonSchema = new Schema.Parser().parse("""{
                                                   "type":"record",
                                                   "name":"Tweet",
                                                   "namespace":"avro",
                                                   "fields":[
                                                      {
                                                         "name":"name",
                                                         "type":"string"
                                                      },
                                                      {
                                                         "name":"text",
                                                         "type":"string"
                                                      }
                                                   ]
                                                }"""")
  val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toJson[Tweet](jsonSchema).invert(x._2).toOption)
  val hashTags = tweets.flatMap(status =>  status.getText.split(" ").filter(_.startsWith("#")))

  val wordCounts = tweets.flatMap(_.getText.split(" ")).map((_,1)).reduceByKey(_ + _)

  val countsSorted = wordCounts.transform(_.sortBy(_._2, ascending = false))

  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                       .map{case (topic, count) => (count, topic)}
                       .transform(_.sortByKey(false))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                       .map{case (topic, count) => (count, topic)}
                       .transform(_.sortByKey(false))

//	 Print popular hashtags

  topCounts60.foreachRDD(rdd => {

      val topList = rdd.take(10)

      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))

      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}

  })

  topCounts10.foreachRDD(rdd => {

        val topList = rdd.take(10)

        println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))

        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}

      })

  sc.start()
  sc.awaitTermination()
}
