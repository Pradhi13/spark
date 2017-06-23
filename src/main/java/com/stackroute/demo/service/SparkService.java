package com.stackroute.demo.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.stereotype.Service;

import kafka.serializer.StringDecoder;

@Service
public class SparkService {
	
	
	public void sparkListener(String topic)
	{
		
		
	    System.out.println("inside service");
	    SparkConf sparkConf = new SparkConf().setAppName("sparkListener").setMaster("local[*]");
	    
	    System.out.println("after sparkConf");
	    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
	    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(2000));
	    
	   
	    
	    System.out.println("it started");

	    Map<String, Object> props = new HashMap<>();
		
		props.put("group.id", "group-1");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
	    System.out.println("after kafka config");
	   // Set<String> topics = Collections.singleton(topic);
	    
	    
	    System.out.println("before javaInputDstream");
//	    JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(javaStreamingContext,
//	            String.class, String.class, props, topic);
//	    
	    
	    JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = 
	    		KafkaUtils.createDirectStream(
			    javaStreamingContext,
			    LocationStrategies.PreferConsistent(),
			    ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topic), props)
			  );
//	    
//	    JavaPairInputDStream<String, String> d = KafkaUtils.
//	    
//	    JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(
//	            javaStreamingContext,
//	            String.class,
//	            String.class,
//	            StringDecoder.class,
//	            StringDecoder.class,
//	            props,
//	            topic
//	    );
	    
	    
	    
	    directKafkaStream.print();
	    System.out.println("before start");
	    javaStreamingContext.start();
	    System.out.println("after start");
	    
	    try {
			javaStreamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    System.out.println("before directKafkaStream");
	    directKafkaStream.foreachRDD(rdd ->{
	    	System.out.println("Rdd with"+ rdd.partitions().size()
	    			+ "partitions and "+ rdd.count()+"records");
	    	
	    	rdd.foreach(record -> System.out.println(2));
	    });
	    
	    System.out.println("before stop");
	    
	    javaStreamingContext.stop();

	}	

}
