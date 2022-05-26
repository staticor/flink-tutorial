
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object WordCountBatch {


  def main(args: Array[String]): Unit = {

    // 执行环境
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // source (file or stream)
    val txtStream : DataSet[String] = env.readTextFile("./input/words.txt")

    txtStream.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
//
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
//
//
//    val stream = env.socketTextStream("localhost", 18899)
//
//    val rstDataStream: DataStream[(Stream, Int)]= stream.flatMap(_.split(" "))
//      .filter(_.nonEmpty)
//      .map((_, 1))
//      .keyBy(0)
//      .sum(1)
//
//    rstDataStream.print()


  }
}