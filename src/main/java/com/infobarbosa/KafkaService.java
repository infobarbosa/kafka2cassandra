package com.infobarbosa;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaService{
	
	private static KafkaService kafkaService;
	private static CassandraService cassandraService;
	private static KafkaConsumer<String, String> consumer;
	private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
	private static final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");

	private KafkaService(){	
		cassandraService = CassandraService.getInstance();
		Properties props = new Properties();
		props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
		props.put("group.id", "kafka2cassandra");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>( props );
		consumer.subscribe( Arrays.asList( KAFKA_TOPIC ) );
	}

	/**Get a instance of KafkaService*/
	public static KafkaService getInstance(){
		if( kafkaService == null ){
			kafkaService = new KafkaService();
		}

		return kafkaService;
	}

	/**Collect tweets from kafka topic*/
	public void poll(){

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
		 	for (ConsumerRecord<String, String> record : records){
		 		cassandraService.save(record.key(), record.value());
			}
		}

	}

	/**Close the service*/
	public void close(){
		if( kafkaService != null) kafkaService = null;
	}
}