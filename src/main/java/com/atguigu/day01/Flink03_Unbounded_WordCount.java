package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_Unbounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1，为了方便查看
        env.setParallelism(1);

        //2.从端口（无界）读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将读过来的数据按照空格切分，切出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.将每一个单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //5.将相同单词的数据聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //6.做累加操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute();

    }
}
