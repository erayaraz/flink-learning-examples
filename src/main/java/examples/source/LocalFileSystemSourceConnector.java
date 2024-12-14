package examples.source;


import examples.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LocalFileSystemSourceConnector {

    /*
        * StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()): Initializes a local execution environment
        for Apache Flink, enabling a Web UI for monitoring the job execution.

        * FileSource<String> fileSource: Defines a source to read data from a file. Multiple variations of FileSource are commented out
        to illustrate different ways to configure file reading in Flink:

            - TextLineInputFormat: Used to read text files line by line as strings. Each line is treated as a single record.
            - monitorContinuously(Duration.ofSeconds(10)): Configures the file source to continuously monitor the file system for new data every 10 seconds.

        * CsvReaderFormat.forPojo(Person.class): Defines a CSV reader format for reading CSV files and converting each row into a `Person` POJO
        (Plain Old Java Object). This simplifies working with structured CSV data.

        * FileSource.forRecordStreamFormat(csvReaderFormat, new Path("csv")): Creates a `FileSource` using the CSV reader format to process CSV files
        at the specified path ("csv"). This setup is configured to monitor the file system for new data.

        * FileSource.forBulkFileFormat(new StreamFormatAdapter<>(csvReaderFormat), new Path("csv")): Creates a `FileSource` for bulk file processing
        using a `StreamFormatAdapter` for the CSV reader. This is useful when working with a batch of CSV files.

        * WatermarkStrategy.noWatermarks(): Specifies that no watermarks are used for event-time processing. This simplifies processing when
        time-based operations are not required.

        * env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source"): Adds the file source to the Flink environment as a data source.
        The source is labeled "file_source" for identification.
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /*FileSource<String> fileSource = FileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), new Path("input"))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();*/

        /*FileSource<String> fileSource = FileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), new Path("input"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();*/

        /*CsvReaderFormat<Person> csvReaderFormat = CsvReaderFormat.forPojo(Person.class);
        FileSource<Person> fileSource = FileSource.forRecordStreamFormat(csvReaderFormat, new Path("csv"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();*/

        CsvReaderFormat<Person> csvReaderFormat = CsvReaderFormat.forPojo(Person.class);

        FileSource<Person> fileSource = FileSource.forBulkFileFormat(
                        new StreamFormatAdapter<>(csvReaderFormat), new Path("csv"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();

        env.execute("file source connector");
    }
}