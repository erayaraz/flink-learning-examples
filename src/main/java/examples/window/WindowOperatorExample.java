package examples.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowOperatorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split("\\s+");
                    return Tuple2.of(values[0], Long.parseLong(values[1]));
                }, Types.TUPLE(Types.STRING, Types.LONG))
                //keyed window
                .keyBy(t -> t.f0)
                //time-based
                //1.tumbling window
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                //2. sliding window
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(5)))
                //3. session window
                //.window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                //4. global window
                //.window(GlobalWindows.create())
                //count-based
                //5. count tumbling window
                //.countWindow(10)
                // count sliding window
                .countWindow(10, 3)
                //no keyed window
                //.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .sum(1)
                .print();
        env.execute("windowing");
    }
}