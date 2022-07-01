package com.atguigu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_Unbounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        为了方便查看UI界面
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //TODO env全局指定
        // 并行度设置为1，为了方便查看
        env.setParallelism(1);

        //全局都不串
//        env.disableOperatorChaining();

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
        })
                //与前面都断开
//                .startNewChain()
                //与前后都断开
//                .disableChaining()
                //TODO 算子指定
//                .setParallelism(3)
                //设置新的共享组（共享的是slot）
                .slotSharingGroup("group1")
                ;

        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = wordToOneDStream.map(r -> r).returns(Types.TUPLE(Types.STRING, Types.INT));

        //5.将相同单词的数据聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = returns.keyBy(0);

        //6.做累加操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute();

    }
}
