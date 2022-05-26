package atgg.basic01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Project: my-flink-project
 * Package: atgg
 * <p>
 * User: Staticor
 * Date: 2022/5/7
 * Time: 14:41
 * <p>
 * Created with IntelliJ IDEA
 * author: staticor
 */


public class FlatmapTest {

    public static void main(String[] args) throws Exception {
        //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("jack", "home", 123L) );
        events.add(new Event("jessy", "home", 1234L) );
        events.add(new Event("james", "home", 1235L) );

        // 从集合中读取数据，只是用于本地测试
        DataStreamSource<Event> stream0 =env.fromCollection(events);

        // 从元素中读取数据
        DataStreamSource<Event> stream1 = env.fromElements(
                new Event("mary", "home", 123L)
                , new Event("zhangsan", "product", 2354L)
                , new Event("haha", "kaka", 100L));

        // 提取user
        SingleOutputStreamOperator<String> result = stream1.flatMap(new MyFlatMapper());

        // lambda
        stream1.map( data -> data.user );

        result.print();


        env.execute();
    }




    public static class MyFlatMapper implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
        }
    }

}