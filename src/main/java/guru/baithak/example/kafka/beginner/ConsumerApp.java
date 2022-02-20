package guru.baithak.example.kafka.beginner;

public class ConsumerApp {

    public static void main( String[] args )
    {

        System.out.println( "Kafka Consumer Demo ! " );
        Consumer consumer=new Consumer();

        consumer.setProperties();
        consumer.createConsumer();
        consumer.subscribeTopic();
        consumer.getMessage();

    }
}
