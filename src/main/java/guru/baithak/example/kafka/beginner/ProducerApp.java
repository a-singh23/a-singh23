package guru.baithak.example.kafka.beginner;

/**
 * Hello world!
 *
 */
public class ProducerApp
{
    public static void main( String[] args )
    {

        System.out.println( "Kafka Producer Demo ! " );

        Producer producer=new Producer();
        producer.setProperties();
        producer.createProducer();
        producer.createRecord("Sending second record to kafka !");
        //producer.sendMessageToTopic();
        producer.sendMessageToTopicWithCallback();
    }
}
