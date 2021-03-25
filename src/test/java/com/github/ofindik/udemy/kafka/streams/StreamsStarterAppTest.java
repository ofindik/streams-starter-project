package com.github.ofindik.udemy.kafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class StreamsStarterAppTest {

	TopologyTestDriver testDriver;

	@Before
	public void setUpTopologyTestDriver () {
		Properties config = new Properties ();
		config.put (StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		config.put (StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());
		config.put (StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ());

		StreamsStarterApp streamsStarterApp = new StreamsStarterApp ();
		Topology topology = streamsStarterApp.createTopology ();
		testDriver = new TopologyTestDriver (topology, config);
	}

	@After
	public void closeTestDriver () {
		testDriver.close ();
	}

	@Test
	public void dummyTest () {
		String dummy = "Du" + "mmy";
		assertEquals (dummy, "Dummy");
	}

	@Test
	public void makeSureCountsAreCorrect () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("word-count-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic ("word-count-output",
			new StringDeserializer (), new LongDeserializer ());

		String firstExample = "testing Kafka Streams";
		pushNewInputRecord (inputTopic, firstExample);
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("testing", 1L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("kafka", 1L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("streams", 1L)));

		String secondExample = "testing Kafka again";
		pushNewInputRecord (inputTopic, secondExample);
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("testing", 2L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("kafka", 2L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("again", 1L)));
	}

	@Test
	public void makeSureWordsBecomeLowercase () {
		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic ("word-count-input",
			new StringSerializer (), new StringSerializer ());
		TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic ("word-count-output",
			new StringDeserializer (), new LongDeserializer ());

		String upperCaseString = "KAFKA kafka Kafka";
		pushNewInputRecord (inputTopic, upperCaseString);
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("kafka", 1L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("kafka", 2L)));
		assertThat (outputTopic.readKeyValue (), equalTo (new KeyValue<> ("kafka", 3L)));
	}

	private void pushNewInputRecord (TestInputTopic<String, String> inputTopic, String value) {
		inputTopic.pipeInput (null, value);
	}
}
