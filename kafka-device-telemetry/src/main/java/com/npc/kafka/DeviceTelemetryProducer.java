package com.npc.kafka;

import java.io.InputStream;
import java.time.Instant;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncp.kafka.DeviceTelemetrySchema;

public class DeviceTelemetryProducer {
	public static void main(String[] args) 
	{
		try(InputStream ins = DeviceTelemetryProducer.class.getResourceAsStream("/device-telemetry-schema.json"))
		{
			JSONObject rawSchema = new JSONObject(new JSONTokener(ins));
			Schema schema = SchemaLoader.load(rawSchema);
			
			//Producer Configuration
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.ACKS_CONFIG, "all");
			props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
			props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
			props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(32*1024*1024));
			props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
			props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
			props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
			props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(120000));
			props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(30000));
			
			KafkaProducer<String, String> producer = new KafkaProducer<>(props);
			
			Scanner sc = new Scanner(System.in);
			
			DeviceTelemetrySchema dts = new DeviceTelemetrySchema();
			
			System.out.println("Enter deviceId: ");
			String deviceId = sc.nextLine();
			
			dts.setTimestamp(Instant.now().toString());
			
			System.out.println("Set temperature: ");
			double temperature = sc.nextDouble();
			sc.nextLine();
			
			System.out.println("set humidity: ");
			double humidity = sc.nextDouble();
			sc.nextLine();
			
			System.out.println("set status (ONLINE, OFFLINE, ERROR): ");
			String status = sc.nextLine();
			
			sc.close();
			
			dts.setDeviceId(deviceId);
			dts.setTemperature(temperature);
			dts.setHumidity(humidity);
			dts.setStatus(status);
			
			ObjectMapper mapper = new ObjectMapper();
			
			String jsonValue = mapper.writeValueAsString(dts);
			schema.validate(new JSONObject(jsonValue));
			
			ProducerRecord<String, String>  record = new ProducerRecord<>("device-telemetry",dts.getDeviceId(), jsonValue);
			
			producer.send(record, (Metadata, exception) -> {
				if(exception == null)
				{
					System.out.println("Record sent successfully to topic " + Metadata.topic() + " partition " + Metadata.partition() + " offset " + Metadata.offset());
				}
				else
				{
					System.out.println("Error sending record: " + exception.getMessage());
				}
			});
			
			producer.close();
		}
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
