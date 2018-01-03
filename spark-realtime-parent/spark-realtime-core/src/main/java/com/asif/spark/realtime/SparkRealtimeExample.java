package com.asif.spark.realtime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkRealtimeExample {

	public static void main(String args[]) {
		 SparkConf conf = new SparkConf()
	                .setAppName("SparkStreamingKafkaExample");
	        JavaStreamingContext jssc =
	                new JavaStreamingContext(conf, Durations.seconds(2));
	        String brokers = "localhost:9092";
	        String zookeeper = "localhost:2181";
	        String topics = "SparkTest";
	        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
	        
	        HashMap<String, String> kafkaParams = new HashMap<String, String>();
	        kafkaParams.put("metadata.broker.list", brokers);
	        kafkaParams.put("zookeeper.connect", zookeeper);
	        
	        // Create direct kafka stream with brokers and topics
	        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	            jssc,
	            String.class,
	            String.class,
	            StringDecoder.class,
	            StringDecoder.class,
	            kafkaParams,
	            topicsSet
	        );
	        
	        // Get the lines, split them into words, count the words and print
	        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>(){
				public String call(Tuple2<String, String> line) throws Exception {
					System.out.println(line._2());
					return line._2();
				}
	        	
	        });
	        
	        lines.print();

	        // Execute the Spark workflow defined above
	        jssc.start();
	        jssc.awaitTermination();
	}

}
