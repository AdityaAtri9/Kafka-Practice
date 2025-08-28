package com.npc.kafka;

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
import com.ncp.kafka.DeviceTelemetrySchema;

public class DeviceTelemetryConsumer {
	public static void main(String[] args)
	{
		try(InputStream ins = DeviceTelemetryConsumer.class.getResourceAsStream("/device-telemetry-schema.json"))
		{
			
			JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
			Schema schema = SchemaLoader.load(rawSchema);
			
			//Consumer configurations
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "device-telemetry-group");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			
			// Manual commit for reliability
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			// Relevant only when producer uses transaction
			props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
			props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "200000");
			
			try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props))
			{
				consumer.subscribe(Collections.singleton("device-telemetry"));
				
				ObjectMapper mapper = new ObjectMapper();
				
				System.out.println("Listening for device telemetry......");
				
				while(true)
				{
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for(ConsumerRecord<String, String> record : records)
					{
						try
						{
							schema.validate(new JSONObject(record.value()));
							
							DeviceTelemetrySchema dt = mapper.readValue(record.value(), DeviceTelemetrySchema.class);
							
							System.out.println("Received Device data: " + dt);
							
							// Commit offset after successful processing
							consumer.commitSync();
						}
						
						catch(Exception e)
						{
							e.printStackTrace();
							System.out.println(e.getMessage());
						}
						
					}
				}
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		
	}

}