package examples.basic;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

// nc -lk 4567 -> first, we start listening on the port (we run this from the terminal)
public class WordCountLambdaExpressionProcess {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 4567)
                .flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
                    Arrays.stream(line.split("\\s+"))
                            .map(w -> Tuple2.of(w, 1L))
                            .forEach(collector::collect);
                }, Types.TUPLE(Types.STRING, Types.LONG))
                //.returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
