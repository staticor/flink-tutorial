package myflink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCountBatch {

  public static void main(String[] args) throws Exception {

      // 1. 创建执行环境

      ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

      // 2. 从本地文件读取数据
      DataSource<String> stringDataSource = env.readTextFile("input/words.txt");

      // 3. 将每行数据 分词
      FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = stringDataSource.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
          String[] words = line.split(" ");
          for (String word : words) {
              collector.collect(new Tuple2(word, 1L));
          }
      }).returns(Types.TUPLE(Types.STRING, Types.LONG));


      // 4. 按 word 分组
      UnsortedGrouping<Tuple2<String, Long>> wordAndGroup = wordAndOneTuple.groupBy(0);

      // 5. 分组内进行聚合统计
      AggregateOperator<Tuple2<String, Long>> aggResult = wordAndGroup.sum(1);



      aggResult.print();



//      env.execute();
  }
}