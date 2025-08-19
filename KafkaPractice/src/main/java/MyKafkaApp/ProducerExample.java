package MyKafkaApp;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerExample 
{
	public static void main(String[] args) 
	{
		// Setting the properties
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Creating Kafka Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		//Sending Message
		ProducerRecord<String, String> record = new ProducerRecord<>("practice-topic2", "This message is sent by producer in using java");
		
		//send method sends the message asynchronously
		producer.send(record, ((metadata, exception) -> {
			if(exception == null)
			{
				System.out.println("Sent to the topic: " + metadata.topic() + " Partition: " + metadata.partition());
			}
		}));
		
		producer.close();
	}
}
