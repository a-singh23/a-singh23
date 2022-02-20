package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    Logger logger=Logger.getLogger(Consumer.class);


    Properties properties=null;
    String BOOTSTRAP_SERVER="127.0.0.1:9092";
    KafkaConsumer<String, String> consumer =null;
    String TOPIC_NAME="logs";
    String GROUP_ID="my-first-app";
    public Consumer(){
        properties=new Properties();
    }

    public void setProperties() {

        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        logger.info("Properties set");

    }

    public void createConsumer() {
        consumer = new KafkaConsumer<String, String>(properties);
        logger.info("Consumer created !");
    }
    public void subscribeTopic(){
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        logger.info("Consumer subscribed to " + TOPIC_NAME + " !");
    }

    public void getMessage() {
        ConsumerRecords<String, String> records =null;
        while(true) {
            records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord record:records) {
                logger.info(" -------- key " + record.key() + " ### value: " + record.value());
            }
        }
    }
}
