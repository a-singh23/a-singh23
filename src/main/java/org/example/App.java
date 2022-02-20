package org.example;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( "Kafka Demo " );

        Producer producer=new Producer();
        producer.setProperties();
        producer.createProducer();
        producer.createRecord("Sending first record to kafka !");
        producer.sendMessageToTopic();
    }
}
