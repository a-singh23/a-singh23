package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;


import java.util.Properties;

public class Producer {

    Logger logger=Logger.getLogger(Producer.class);

    Properties properties=null;
    String bootstrapServers="127.0.0.1:9092";
    KafkaProducer<String, String> producer =null;
    ProducerRecord record =null;

    public Producer(){
        properties=new Properties();
    }

    void setProperties() {
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        logger.info("Properties set");
    }

    void createProducer() {

        producer = new KafkaProducer<String, String>(properties);
        logger.info("Producer created !");
    }

    void createRecord(String message) {
        //Create a Producer record
        record = new ProducerRecord("logs",message);


    }
    void sendMessageToTopic() {
        producer.send(record);
        producer.flush();
        logger.info("Record sent to producer !");
    }


}
