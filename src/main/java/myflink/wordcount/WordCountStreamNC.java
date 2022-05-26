package myflink.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 05.05
 * author: staticor
 */
public class WordCountStreamNC {

  public static void main(String[] args) throws Exception {

      // 1 创建流式执行环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      env.disableOperatorChaining();

//      env.setRuntimeMode(RuntimeExecutionMode.BATCH);   不建议

     ParameterTool parameterTool = ParameterTool.fromArgs(args);

//     String hostname = parameterTool.get("host");
//     Integer port = parameterTool.getInt("port");

      DataStreamSource<String> t2 = env.socketTextStream("localhost", 18899);
//      DataStreamSource<String> t2 = env.socketTextStream(hostname, port);

      // 3.转换计算
      SingleOutputStreamOperator<Tuple2<String, Long>> t3 = t2.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
          String[] words = line.split("[\\s\\/]");
          for (String word : words) {
              out.collect(new Tuple2(word, 1L));
          }
      }).returns(Types.TUPLE(Types.STRING, Types.LONG));

      // 4. 分组
//      words.keyBy(0);
      KeyedStream<Tuple2<String, Long>, String> keyedBY = t3.keyBy(data -> data.f0);

      keyedBY.sum(1).print();

      env.execute();
  }
}