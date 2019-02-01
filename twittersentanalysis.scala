package main.project.twittersentimentanalysis

import java.time.format.DateTimeFormatter
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._

object twittersentanalysis 
{
  def main(args:Array[String])
  {
    val spark = SparkSession.builder.master("local[*]").appName("twitter Sentiment Analysis").getOrCreate()
    val stopWords = spark.sparkContext.textFile("file:/home/hduser/stopwords")
    val stopWordSet = stopWords.collect.toSet
    val stopWordSetBC = spark.sparkContext.broadcast(stopWordSet)
    
    val twitterkeys = Array("fGXIBeK55puGqlt8fLNQVkbuC","4ejpp8AnIfPy6nehJiIj7PITOWMairXodItSjOQTwA8Su2IOuM","2952347630-KJ8BO0VoMG46YqRyvCY3c0fX7YNc0nWbg9zzw4i","a4fZtWxlUQaWP4kZuwLpQIBLgeeXluwSau5x3HGBinpEl")
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = twitterkeys
    val filters = Array("amazon great indian festival","amazon india","flipkart","amazon")
    val cb = new ConfigurationBuilder
     cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setHttpConnectionTimeout(10000)
    val auth = new OAuthAuthorization(cb.build)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)
    tweets.foreachRDD{(rdd, time) =>
       rdd.map(t => {
         Map(
           "user"-> t.getUser.getScreenName,
           "created_at" -> t.getCreatedAt.toInstant.toString,
           "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
           "tweettext" -> t.getText,
           "tweettextanalysed" -> stopwordsremoval(t.getText),
           "hashtags" -> t.getHashtagEntities.map(_.getText),
           "retweet" -> t.getRetweetCount,
           "language" -> t.getLang,
           "sentiment" -> sautils.detectSentiment(t.getText).toString
         )
       }).saveToEs("satwitter/tweet")
     }
     ssc.start()
     ssc.awaitTermination()
     def stopwordsremoval(str:String) = 
     {
        str.split("\\W").filter(x => !stopWordSetBC.value.contains(x.toLowerCase().trim()))
        .fold("")((a,b) => a + " " + b)
     }
  }
}