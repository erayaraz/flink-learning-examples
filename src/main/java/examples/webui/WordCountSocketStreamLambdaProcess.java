package examples.webui;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

// nc -lk 4567 -> first, we start listening on the port (we run this from the terminal)
public class WordCountSocketStreamLambdaProcess {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .flatMap((String line, Collector<String> collector) -> {
                    Arrays.stream(line.split("\\s+")).forEach(collector::collect);
                }, Types.STRING)
                .map(word -> Tuple2.of(word, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
