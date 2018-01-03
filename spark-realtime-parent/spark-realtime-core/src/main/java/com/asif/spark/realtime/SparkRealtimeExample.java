package com.asif.spark.realtime;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 ** 
 *This program can be executed by following command from your local build jar location
 * spark-submit --master yarn --deploy-mode cluster --class com.asif.spark.realtime.SparkRealtimeExample 
 * spark-realtime-core-jar-with-dependencies.jar localhost:9092 localhost:2181 SparkTest
 * 
 */
public class SparkRealtimeExample implements Serializable {

	private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";
	private final static String TOPIC = "kafka-streaming-output";
	
	private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaSparkExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        		StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }
	
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
	        
	        final Producer<String, String> producer = createProducer();
	        
	        // Get the lines, split them into words, count the words and print
	        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>(){
				public String call(Tuple2<String, String> line) throws Exception {
					System.out.println(line._2());
					
					final ProducerRecord<String, String> record =
		                      new ProducerRecord<String, String>(TOPIC, "key",
		                    		  line._2());
		              RecordMetadata metadata = producer.send(record).get();

					return line._2();
				}
	        	
	        });
	        
	        lines.print();

	        // Execute the Spark workflow defined above
	        jssc.start();
	        jssc.awaitTermination();
	}

}
