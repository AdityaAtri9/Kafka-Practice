package com.ncp.kafka;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderEventConsumer 
{
	public static void main(String[] args) 
	{
		try(InputStream ins = OrderEventConsumer.class.getResourceAsStream("/order-event-schema.json"))
		{
			JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
			Schema schema = SchemaLoader.load(rawSchema);
			
			// Kafka Consumer Configuration
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
				// Subscribing to the topic
				consumer.subscribe(Collections.singleton("order-events"));
				
				ObjectMapper mapper = new ObjectMapper();
				
				System.out.println("Listening for order events.....");
				
				while(true)
				{
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
					for(ConsumerRecord<String, String> record : records)
					{
						try {
						schema.validate(new JSONObject(record.value()));	
						
						OrderEventSchema event = mapper.readValue(record.value(), OrderEventSchema.class);
						
						System.out.println("Received order event: " + event);
						}
						catch (Exception e)
						{
							System.out.println("Invalid Event......" + record.value());
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
