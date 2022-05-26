package atgg.basic01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * Project: my-flink-project
 * Package: atgg
 * <p>
 * User: Staticor
 * Date: 2022/5/7
 * Time: 12:31
 * <p>
 * Created with IntelliJ IDEA
 * author: staticor
 */
public class SourceTest {

  public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      env.setParallelism(1);

      // 1..从文件读取数据。
      DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

      // 2.从集合中读取数据
      ArrayList<Integer> nums = new ArrayList<>();
      for(int i = 0; i< 10; i ++) nums.add(new Random().nextInt(10));
      DataStreamSource<Integer> stream2 = env.fromCollection(nums);

      ArrayList<Event> events = new ArrayList<>();
      for(int i = 0; i< 10; i ++){
          events.add(new Event("zhangsan", "./home", new Random().nextLong()));
      }

      DataStreamSource<Event> stream3 = env.fromCollection(events);

      DataStreamSource<String> stream4 = env.socketTextStream("localhost", 9988);
      stream1.print();
      stream2.print();
      stream3.print();
      stream4.print();


      // Kafka
      String topic = "clicks";
      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", "localhost:9092");

      properties.setProperty("group.id", "cg-test");
      properties.setProperty("auto.offset.reset", "latest");

      DataStreamSource<String> kafkastream = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties));

      kafkastream.print();

      // 自定义soruce function

      DataStreamSource<Event> stream6 = env.addSource(new ClickSource()).setParallelism(1);
        // Exception in thread "main" java.lang.IllegalArgumentException: The parallelism of non parallel operator must be 1.

      stream6.print();
      env.execute();
  }
}