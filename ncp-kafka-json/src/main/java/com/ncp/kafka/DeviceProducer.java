package com.ncp.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DeviceProducer {
	
	public static void main(String[] args) 
	{
		//Loading JSON schema from resources
		try(InputStream inputstream = DeviceProducer.class.getResourceAsStream("/device-schema.json"))
		{
			JSONObject rawSchema = new JSONObject(new JSONTokener(inputstream));
			Schema schema = SchemaLoader.load(rawSchema);
			
			//kafka Producer Configuration
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			
			KafkaProducer<String, String> producer = new KafkaProducer<>(props);
			
			//Creating device status object
			DeviceStatus status = new DeviceStatus("device-001", "online", Instant.now().toString());
			
			//Convert to JSON
			ObjectMapper mapper = new ObjectMapper();
			String jsonValue = mapper.writeValueAsString(status);
			
			//Validation JSON against Schema
			schema.validate(new JSONObject(jsonValue));
			
			//Send to Kafka
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("device-status",status.getDeviceId(), jsonValue);
			producer.send(record);
			
			System.out.println("Record sent successfully: " + jsonValue);
			
			producer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
