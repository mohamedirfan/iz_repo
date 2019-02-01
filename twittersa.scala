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

object twittersa {
  case class tweetinfo(user:String,created_at:String,tweettext:String,
      tweettextanalysed:String,retweet:Int,language:String,sentiment:String)
  def main(args:Array[String])
  {
    
    val spark = SparkSession.builder.master("local[*]").appName("twitter Sentiment Analysis").getOrCreate()
    val twitterkeys = Array("fGXIBeK55puGqlt8fLNQVkbuC","4ejpp8AnIfPy6nehJiIj7PITOWMairXodItSjOQTwA8Su2IOuM","2952347630-KJ8BO0VoMG46YqRyvCY3c0fX7YNc0nWbg9zzw4i","a4fZtWxlUQaWP4kZuwLpQIBLgeeXluwSau5x3HGBinpEl")
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = twitterkeys
    val filters = Array("verizon","infosys")
    val cb = new ConfigurationBuilder
     cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setHttpConnectionTimeout(10000)
    import spark.implicits._
    val auth = new OAuthAuthorization(cb.build)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
  .setInputCol("tweettext")
  .setOutputCol("words")
  .setPattern("\\W")
    
    tweets.foreachRDD((rdd, time) => {
     val df = rdd.map(t => 
       tweetinfo(
         t.getUser.getScreenName,
         t.getCreatedAt.toInstant.toString,
         t.getText,
         t.getText,         
         t.getRetweetCount,
         t.getLang,
         sautils.detectSentiment(t.getText).toString)).toDF
    
    })
     ssc.start()
     ssc.awaitTermination()
  }
  
}