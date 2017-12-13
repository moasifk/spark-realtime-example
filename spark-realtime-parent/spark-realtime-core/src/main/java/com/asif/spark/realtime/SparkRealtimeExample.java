package com.asif.spark.realtime;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkRealtimeExample {

	public static void main(String args[]) {
		 SparkConf conf = new SparkConf()
	                .setMaster("local[*]")
	                .setAppName("VerySimpleStreamingApp");
	        JavaStreamingContext streamingContext =
	                new JavaStreamingContext(conf, Durations.seconds(5));
	        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("", 9999);
	        
	        lines.print();

	        // Execute the Spark workflow defined above
	        streamingContext.start();
	        streamingContext.awaitTermination();
	}

}
