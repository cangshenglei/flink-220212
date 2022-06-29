package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.按照空格切分切出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格切分
                String[] words = value.split(" ");
                //遍历出每一个单词
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
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //6.做累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //7.打印到控制台
        result.print();

        //会触发操作 相当于提交了一个job
        env.execute();
    }
}
