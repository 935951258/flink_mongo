package com.event.ingestion.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;

import com.event.ingestion.common.Persistable;
import com.event.ingestion.config.LoadConfig;
import com.event.ingestion.ingestionExecutor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.fs.Path;



public abstract class IT21Consumer implements ingestionExecutor {
	//property - kafka broker url
	private String kafkaBrokerUrl = null;
	//kafka topic
	protected abstract String getKafkaTopic();
	//the flag for how to commit the consumer reads
	protected abstract Boolean getKafkaAutoCommit();
	//the max # of records polled
	protected int getMaxPolledRecords() {
		return 900;
	}
	//the max # of records polled
	protected int getMaxPollIntervalMillis() {
		return 1200;
	}
	//consumer group
	protected abstract String getKafkaConsumerGrp();

	//all writers
	private Persistable[] writers;

	//constructor
	public IT21Consumer() {
		//initialize the writers
		this.writers = this.getWriters();
	}
	
	//initialize the properties
	public void initialize(Properties props) {
		//load
		this.kafkaBrokerUrl = props.getProperty(LoadConfig.kafkaBrokerUrl);
		//initialize
		for ( Persistable writer: this.writers ) {
			//call
			writer.initalize(props);
		}
	}

	//consume
	protected void consume() throws Exception {
		//check
		if ( this.kafkaBrokerUrl == null || this.kafkaBrokerUrl.isEmpty() ) {
			//error out
			throw new Exception("The Kafka broker url is not initialized.");
		}
		//print
		System.out.println("The Kafka BrokerUrl --> " + this.kafkaBrokerUrl);
		//prepare properties for the consumer
		Properties props = new Properties();
		//the kafka servers
		props.put("bootstrap.servers", this.kafkaBrokerUrl);
		//the consumer group
		props.put("group.id", this.getKafkaConsumerGrp());
		//flag for whether or not to commit the offset automatically
		props.put("enable.auto.commit", this.getKafkaAutoCommit() ? "true" : "false");
		//default option for resetting the offset
		props.put("auto.offset.reset", "earliest");  //another option: latest
		//the max # of records for each poll
		props.put("max.poll.records", Integer.toString(this.getMaxPolledRecords()));
		//the amount of time for the processing thread to process the current batch of messages
		props.put("max.poll.interval.ms", Integer.toString(this.getMaxPollIntervalMillis()));
		//key & value serializer
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		//subscribe
		consumer.subscribe(Arrays.asList(new String[] { getKafkaTopic() }));
		//print message
		System.out.println("Consumer subscribed to topic -> " + getKafkaTopic());
		try {
			long msgPolled = 0L, msgProcessed = 0L;
			//loop for reading   
			while ( true ) {
			    //poll records
				ConsumerRecords<String, String> records = consumer.poll( 3000 );
				//number of records
				int recordsCount = (records != null) ? records.count() : 0;
				//check
				if ( recordsCount <= 0 ) {
					//sleep for 3 seconds
					Thread.sleep(3000);
					//go next
					continue;
				}
				//add the # of messages polled
				msgPolled += recordsCount;
				
				//writers
				for (Persistable writer: writers) {
					//write
					msgProcessed += writer.write( records );
				}

				//check
				if ( !this.getKafkaAutoCommit() ) {
					//commit
					consumer.commitSync();
				}
				//print
				System.out.print(String.format("**** %d messages polled, %d processed! -----", msgPolled, msgProcessed));
			}  
		}
		finally {
			//close
			consumer.close();
		}
	}

	//main entry
	public void execute(String[] args) throws Exception {
		//check
		if (args.length < 1) {
			System.out.println(String.format("Usage: %s <settings-file>", this.getClass().getName()));
			System.out.println("<settings-file>: the configuration settings");
		}
		else {
			//initialize
			this.initialize(LoadConfig.loadSettings(args[0]));
			//consume & persist to hbase
			this.consume();
		}
	}

	//writers
	protected abstract Persistable[] getWriters();
}
