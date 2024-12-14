package examples.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/*
    * StreamExecutionEnvironment.createLocalEnvironmentWithWebUI: Provides an environment for Flink to perform stream processing operations.
    This method creates a local environment and also allows monitoring of the execution via a Web UI.

    * fromCollection(Arrays.asList(1, 2, 3, 4, 5)): Starts a data stream from a collection. In this case, a list created with Arrays.asList is used.
    Each element is multiplied by 10 in the map operation, and the result is printed to the console.

    * fromElements(1, 2, 3, 4, 5): Creates a source from individual elements. Each element is multiplied by 2 in the map operation,
    and the result is printed to the console.

    * fromSequence(10, 30): Creates a sequence of integers within the specified range (from 10 to 30, inclusive).
    Each element is subjected to a 1-second delay (TimeUnit.SECONDS.sleep(1)) and then multiplied by 3.
    This simulates sequential data processing. The results are printed to the console.
 */


public class CollectionsSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5))
                .map(value -> value * 10, Types.INT)
                .print("collections");

        env.fromElements(1, 2, 3, 4, 5)
                .map(e -> e * 2, Types.INT)
                .print("elements");

        env.fromSequence(10, 30)
                .map(e -> {
                    TimeUnit.SECONDS.sleep(1);
                    return e * 3;
                }, Types.LONG)
                .print("sequence");

        env.execute("Java Collection Source");
    }
}