
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import org.apache.flink.streaming.api.windowing.time.Time

object WordCountStreamNC {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)
//
//    text
//      .flatMap(_.split(","))
//      .map( (_, 1))
//      .keyBy(0)
//      .timeWindow(Time.seconds(5))
//      .sum(1)
//      .print()
//      .setParallelism(1)

    env.execute("Scala App streaming")
  }

}