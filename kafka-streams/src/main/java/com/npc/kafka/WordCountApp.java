package com.npc.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountApp {
	
	private static final Logger logger = LoggerFactory.getLogger(WordCountApp.class);
	
	public static void main(String[] args)
	{
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		// Step 1: Read input Stream
		KStream<String, String> textLines = builder.stream("input-topic");
		logger.info("Reading messages from input-stream...");
		
		// Step 2: Split lines into words
		KTable<String, Long> wordCounts = textLines.flatMapValues(value -> {
			logger.debug("Processing line: {}", value);
			return Arrays.asList(value.toLowerCase().split(" "));
		})
				.groupBy((key, word) -> word)
				.count();
		
		// Step 3: Write results to output-topic
		wordCounts.toStream().to("output-topic",Produced.with(Serdes.String(), Serdes.Long()));
		logger.info("Wrod counts will be written to output-topic");
		
		// Build and start the stream
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
		logger.info("Kafka streams app started successfully!)");
		
		// Shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread (()->{
			logger.info("Shutting down kafka streams app....");
			streams.close();
		}));
	}

}
