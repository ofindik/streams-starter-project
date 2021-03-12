package com.github.ofindik.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
	public static void main (String[] args) {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put (StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());
		config.put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());

		StreamsBuilder streamsBuilder = new StreamsBuilder ();
		// 1 - stream from Kafka
		KStream<String, String> wordCountInput = streamsBuilder.stream ("word-count-input");

		KTable<String, Long> wordCounts = wordCountInput
			// 2 - map values to lowercase
			.mapValues (value -> value.toLowerCase ())
			// 3 - flatmap values split by space
			.flatMapValues (value -> Arrays.asList (value.split (" ")))
			// 4 - select key to apply a key (we discard the old key)
			.selectKey ((key, value) -> value)
			// 5 - group by key before aggregation
			.groupByKey ()
			// 6 - count occurrences
			.count (Named.as ("Counts"));

		// 7 - to in order to write the results back to kafka
		wordCounts.toStream ().to ("word-count-output", Produced.with (Serdes.String (), Serdes.Long ()));

		KafkaStreams kafkaStreams = new KafkaStreams (streamsBuilder.build (), config);
		kafkaStreams.start ();

		// printed the topology
		System.out.println (kafkaStreams.toString ());

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime ().addShutdownHook (new Thread (kafkaStreams::close));
	}
}