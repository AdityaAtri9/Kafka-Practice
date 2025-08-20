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

public class DeviceConsumer {
	
	public static void main(String[] args) 
	{
		//Loading JSON schema from resources
		try(InputStream inputStream = DeviceConsumer.class.getResourceAsStream("/device-schema.json"))
		{
			JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
			Schema schema = SchemaLoader.load(rawSchema);
			
			//Kafka Consumer Configuration
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "device-consumer-group");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
			
			//Subscribing to the topic
			consumer.subscribe(Collections.singletonList("device-status"));
				
			ObjectMapper mapper = new ObjectMapper();
			
			System.out.println("Listening for device status messages....");
			
			while(true)
			{
				//Poll messages
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				for(ConsumerRecord<String, String> record : records)
				{
					try {
						
						//Validate JSON
						schema.validate(new JSONObject(record.value()));
						
						System.out.println("RAW Kafka message: " + record.value());
						
						//Convert to JSON object
						DeviceStatus status = mapper.readValue(record.value(), DeviceStatus.class);
						
						//Print Device info
						System.out.println("Received device update: " + status.getDeviceId() + " is " + status.getStatus() + " at " + status.getTimestamp());
						
						
					} catch (Exception e) {
                        System.err.println("Invalid message: " + record.value());
                        e.printStackTrace();
					}
					
					finally
					{
						if(consumer != null)
						{
							consumer.close();
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
