import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Project: KafkaUseScala
 * Package: PACKAGE_NAME
 * <p>
 * User: Staticor
 * Date: 2022/5/16
 * Time: 15:59
 * <p>
 * Created with IntelliJ IDEA
 * author: staticor
 */
public class SimpleConsumer {

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "Just-a-Test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("may"));
        return consumer;
    }

    public static void runConsumer() throws InterruptedException {
        final Consumer<String, String> consumer = createConsumer();

        final int giveUp = 5;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

  public static void main(String[] args) throws InterruptedException {
      runConsumer();
  }
}