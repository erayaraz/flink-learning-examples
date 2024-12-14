package examples.basic;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountSocketStreamProcess {
    // nc -lk 4567 -> first, we start listening on the port (we run this from the terminal)
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 4567)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split("\\s+")).forEach(word -> out.collect(Tuple2.of(word, 1L)));
                    }
                }).keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
