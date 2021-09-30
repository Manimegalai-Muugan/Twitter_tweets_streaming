package com.mani.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._
import java.util.Arrays;
 

object PrintTweets {
 
  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    
    val statuses = tweets.map(status => status.getText())
   val stream = statuses.flatMap(f=> f.split(" "))
    val streamstartswith = stream.reduce(_ + _).count()

    stream.print()
    
    ssc.start()
    ssc.awaitTermination()
  }  
}