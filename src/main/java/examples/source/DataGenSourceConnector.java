package examples.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/*
    * StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()): Creates a local stream processing environment in Apache Flink
    with a web user interface (Web UI) to monitor and interact with the running job.

    * env.setParallelism(2): Sets the degree of parallelism for the entire program to 2. This determines how many parallel tasks
    will be executed for operators that can run in parallel.

    * GeneratorFunction<Long, String> function: Defines a generator function that transforms a Long input into a String output.
    In this case, it prefixes the input with "Elements->".

    * DataGeneratorSource<String> generatorSource: A data generator source that produces synthetic data of type String.
    It uses the defined generator function to generate data, with a limit of 100 elements and a rate of 10 elements per second
    (as controlled by the RateLimiterStrategy).

    * env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource"): Adds the generatorSource to the data stream
    environment as a source. No watermarks are used, and the source is named "dataGeneratorSource".
 */

public class DataGenSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        GeneratorFunction<Long, String> function = e -> "Elements-> " + String.valueOf(e);
        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(function, 100,
                RateLimiterStrategy.perSecond(10),
                Types.STRING);
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource")
                .print();

        env.execute("DataGenSourceCollectorExample");
    }
}