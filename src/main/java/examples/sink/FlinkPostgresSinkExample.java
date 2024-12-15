package examples.sink;

import examples.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkPostgresSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(2_000, CheckpointingMode.EXACTLY_ONCE);

        Source<Long, ?, ?> source = new NumberSequenceSource(1, 100);

        // PostgreSQL Sink tanımı
        SinkFunction<Person> jdbcSink = JdbcSink.<Person>sink(
                "INSERT INTO person(id, age, name, address) VALUES (?, ?, ?, ?)",
                (st, person) -> {
                    st.setInt(1, person.getId());
                    st.setInt(2, person.getAge());
                    st.setString(3, person.getName());
                    st.setString(4, person.getAddress());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/flinkdb")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("postgres")
                        .withPassword("postgres")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        env.fromSource(source, WatermarkStrategy.<Long>noWatermarks(), "number_sequence_source")
                .map(num -> Person.builder()
                        .id(num.intValue())
                        .age(num.intValue())
                        .address("address " + num)
                        .name("name " + num)
                        .build())
                .addSink(jdbcSink);

        env.execute("Flink PostgreSQL Sink Example");
    }
}