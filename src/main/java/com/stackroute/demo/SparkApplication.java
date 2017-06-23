package com.stackroute.demo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import scala.Tuple2;

@SpringBootApplication
public class SparkApplication {

	public static void main(String[] args) {
		
		System.out.println("inside main");
		SpringApplication.run(SparkApplication.class, args);
		//SparkConf sparkConf = new SparkConf().setAppName("simple app").setMaster("local[2]").set("spark.executor.memory","1g");
		
		//SparkConf sparkConf = new SparkConf().setAppName("simple").setMaster(master);
		//check(sparkConf);
	}
	
	
}
	
	
//	public static void check(SparkConf sparkConf)
//	{
//		String file = "./README.md";
//		
//		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//		JavaRDD<String> lines = javaSparkContext.textFile(file).cache();
////		long numAs = data.filter(new Function<String, Boolean>() {
////		      public Boolean call(String s) { return s.contains("hai"); }
////		    }).count();
//	
//		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
//			  public Integer call(String s) { return s.length(); }
//			});
//		
//		int totallength = lineLengths.reduce(new Function2<Integer,Integer,Integer>(){
//			public Integer call(Integer a, Integer b){return a+b;
//			}
//		});
//		
//		JavaPairRDD<String,Integer> pairs = lines.mapToPair(s -> new Tuple2(s,1));
//		JavaPairRDD<String,Integer> counts = pairs.reduceByKey((a,b) -> a+b);
//		counts.persist(StorageLevel.MEMORY_ONLY());
//		System.out.println(counts.collect());
//		javaSparkContext.stop();
//	}
	

