package com.npc.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordConsumer {

	private static final Logger logger = LoggerFactory.getLogger(WordConsumer.class);
	
	public static void main(String[] args) 
	{
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "word-output-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "200000");
		
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props))
		{
			consumer.subscribe(Collections.singletonList("output-topic"));
			logger.info("Consumer subscribed to output-topic");
			
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("Shutdown requested. Closing consumer...");
				
				try
				{
					consumer.wakeup();
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				
			}));
			
			while(true)
			{
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, String> record : records)
				{
					logger.info("record received -> word='{}' count={} (partition={}, offset={})",record.key(), record.value(), record.partition(), record.offset());
				}
			}
		}
		catch (WakeupException e) 
		{
			logger.error("Consumer wakeup called, exiting loop", e);
		}
		catch (Exception e) {
			logger.error("Consumer error {}", e.getMessage());
		}
		finally {
			logger.info("Consumer stopped.");
		}
		
	}
}
