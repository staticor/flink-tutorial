package org.example;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Project: kafkademo
 * Package: org.example
 * <p>
 * User: Staticor
 * Date: 2022/5/10
 * Time: 15:09
 * <p>
 * Created with IntelliJ IDEA
 * author: staticor
 */
public class SimpleProducer {

    public static void main(String[] args) {


        Properties props = new Properties();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String today = sdf.format(date);



        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);



        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("may"
                    ,0
                    , "key"
                    , "金勇在" + today + "+晚1110点生产信息, value" + Integer.toString(i)));
        }

        producer.close();
    }
}