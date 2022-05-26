
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment



object WordCountStream {


  def main(args: Array[String]): Unit = {

    // 执行环境

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
//
//
    val stream: DataStreamSource[String] = env.socketTextStream("localhost", 18899)
//
//    val rstDataStream = stream.flatMap(_.split(" "))
//      .filter(_.nonEmpty)
//      .map((_, 1))
//      .keyBy(0)
//      .sum(1)
//
//    rstDataStream.print()


  }
}