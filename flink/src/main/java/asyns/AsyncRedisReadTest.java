package asyns;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class AsyncRedisReadTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置检查点时间间隔
        env.enableCheckpointing(5000);

        //设置检查点模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> kafkaData = env.readTextFile("/data/input/ab.txt");

        //kafkaData.print();
        System.out.println("=================================================================");

        SingleOutputStreamOperator<String> unorderedWait = AsyncDataStream.orderedWait(kafkaData, new AsyncReadRedis(), 1000, TimeUnit.MICROSECONDS, 1);

        unorderedWait.print().setParallelism(2);

        //设置程序名称
        env.execute("data_to_redis");

    }

}
