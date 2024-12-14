package examples.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountBatchProcess {
    public static void main(String[] args) throws Exception {
        // 1. Create the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. Read data from batch files
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        // 3. Process and transform data
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = dataSource.flatMap(new Tokenizer());

        UnsortedGrouping<Tuple2<String, Long>> unsortedGrouping = flatMapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> aggregateOperator = unsortedGrouping.sum(1);

        // 4. Print the result
        aggregateOperator.print();
    }

    // Tokenizer class to split lines into words and emit (word, 1) pairs
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
            Arrays.stream(line.split("\\s+")).forEach(
                    word -> collector.collect(Tuple2.of(word, 1L))
            );
        }
    }
}