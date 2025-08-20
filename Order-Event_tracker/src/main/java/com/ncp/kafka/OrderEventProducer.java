package com.ncp.kafka;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
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

public class OrderEventProducer 
{
	public static void main(String[] args) 
	{
		try(InputStream ins = OrderEventProducer.class.getResourceAsStream("/order-event-schema.json"))
				{
					JSONObject rawSchema = new JSONObject( new JSONTokener(ins));
					Schema schema = SchemaLoader.load(rawSchema);
					
					// Kafka Producer Configuration
					Properties props = new Properties();
					props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
					props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
					props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
					props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
					props.put(ProducerConfig.ACKS_CONFIG, "all");
					props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
					props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
					props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
					props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32*1024);
					props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
					props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
					
					KafkaProducer<String, String> producer = new KafkaProducer<>(props);
					
					// Taking User Input
					Scanner sc = new Scanner(System.in);
					
					System.out.println("Enter Order ID: ");
					String OrderId = sc.nextLine();
					
					System.out.println("Enter Customer Name: ");
					String customerName = sc.nextLine();
					
					System.out.println("Enter amount: ");
					Double amount = sc.nextDouble();
					sc.nextLine();
					
					System.out.println("Enter status (NEW/PROCESSING/SHIPPED): ");
					String status = sc.nextLine();
					
					System.out.println("Enter items (comma separated): ");
					String itemsInput = sc.nextLine();
					List<String> items = Arrays.asList(itemsInput.split(","));
					
					sc.close();
					
					// Create order event
					OrderEventSchema order = new OrderEventSchema();
					order.setOrderId(OrderId);
					order.setCustomerName(customerName);
					order.setAmount(amount);
					order.setStatus(status);
					order.setItems(items);
					
					// Converting to JSON object
					ObjectMapper mapper = new ObjectMapper();
					
					String jsonValue = mapper.writeValueAsString(order);
					schema.validate(new JSONObject(jsonValue));
					
					
					// Send to kafka
					//																TOPIC				KEY				VALUE
					ProducerRecord<String, String> record = new ProducerRecord<>("order-events", order.getOrderId(), jsonValue);
					/** Basic meaning of this line is -
					Create a Kafka object for topic "order-events" whose key is order.getId() (String) and whose value is jsonValue (String)
					- ready to be sent by KafkaProducer<String, String>. */
					
					producer.send(record, (Metadata, exception) -> {
						if(exception == null)
						{
							System.out.println("Record is sent to topic " + Metadata.topic() + " partition " + Metadata.partition() + " Offset " + Metadata.offset());
						}
						else
						{
							System.out.println("Error sending record: " + exception.getMessage());
						}
					});
					System.out.println("Order sent successfully");
					producer.close();
				}
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
