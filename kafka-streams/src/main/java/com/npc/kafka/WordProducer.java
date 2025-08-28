package com.npc.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordProducer 
{
	private static final Logger logger = LoggerFactory.getLogger(WordProducer.class);

	public static void main(String[] args) 
	{
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024*1024));
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(120000));
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(30000));
		
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			Scanner sc = new Scanner(System.in);
			
			
			logger.info("WordProducer started. Type sentences and press Enter (or 'exit)'");
			
			while(true)
			{
				String line = sc.nextLine();
				
				if(line == null)
				{
					break;
				}
				
				if("exit".equalsIgnoreCase(line.trim()))
				{
					logger.info("Exit received. Shutting down producer.");
					break;
				}
				
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("input-topic",null, line);
				
				producer.send(record, (Metadata, exception) -> {
					
					if(exception == null)
					{
						logger.info("Record sent successfully to topic={} partition={} offset=P{}", Metadata.topic(), Metadata.partition(), Metadata.offset());
					}
					else
					{
						logger.error("Record send failed.", exception);
					}
				});
			}
			
			sc.close();
			producer.flush();
		}
		catch(Exception e)
		{
			logger.error("Producer error", e);
		}
		
		logger.info("WordProducer Stopper.");
	}

}
