package guru.baithak.example.kafka.beginner;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.slf4j.event.Level;


import java.util.Properties;

public class Producer {

    Logger logger=Logger.getLogger(Producer.class);


    Properties properties=null;
    String BOOTSTRAP_SERVER="127.0.0.1:9092";
    KafkaProducer<String, String> producer =null;
    ProducerRecord record =null;
    String TOPIC_NAME="logs";

    public Producer(){
        properties=new Properties();

    }

    void setProperties() {
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        logger.info("Properties set");
    }

    void setSafeProducerProperties() {
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"180000");

        logger.info("Safe Producer Properties set");
    }

    void createProducer() {

        producer = new KafkaProducer<String, String>(properties);
        logger.info("Producer created !");
    }

    void createRecord(String message) {
        //Create a Producer record
        record = new ProducerRecord(TOPIC_NAME,message);

    }

    void createRecord(String key, String message) {
        //Create a Producer record
        record = new ProducerRecord(TOPIC_NAME,key,message);

    }

    void sendMessageToTopic() {
        producer.send(record);
        producer.flush();
        logger.info("Record sent to producer !");
    }

    void sendMessageToTopicWithCallback() {
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                logger.info(" 1. PARTITION - " + recordMetadata.partition()
                        + " \n 2. TOPIC -  " + recordMetadata.topic()
                        +  " \n 3. SERIALIZED KEY VALUE -  " +recordMetadata.serializedValueSize()
                        +  " \n 4. TIMESTAMP - " +recordMetadata.timestamp());
            }
        });
        producer.flush();
        logger.info("Record sent to producer !");
    }
}
