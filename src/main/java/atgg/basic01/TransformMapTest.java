package atgg.basic01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Project: my-flink-project
 * Package: atgg
 * <p>
 * User: Staticor
 * Date: 2022/5/7
 * Time: 14:24
 * <p>
 * Created with IntelliJ IDEA
 * author: staticor
 */

public class TransformMapTest {

    public static void main(String[] args) throws Exception {
    //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(new Event("mary", "home", 123L)
                , new Event("zhangsan", "product", 2354L)
                , new Event("haha", "kaka", 100L));

        // 提取user
        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

        // lambda
        stream.map( data -> data.user );

        result.print();


        env.execute();
    }


    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}