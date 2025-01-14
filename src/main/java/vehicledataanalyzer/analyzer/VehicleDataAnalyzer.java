package vehicledataanalyzer.analyzer;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import vehicledataanalyzer.analyzer.processor.*;
import vehicledataanalyzer.analyzer.sink.AlarmToDatabaseSink;
import vehicledataanalyzer.model.vehicle.AlarmDataModel;
import vehicledataanalyzer.model.vehicle.VehicleDataModel;

import java.time.Duration;
import java.util.List;

/**
 * Vehicle Data Analyzer: Detects engine warning patterns and triggers alarms.
 * nc -lk 4567 -> first, we start listening on the port (we run this from the terminal)
 * <p>
 * { "plate": "34ABC123", "engineWarning": true, "airbagWarning": true, "hoodOpen": true, "speed": 150, "engineTemperature": 110, "fuelLevel": 2 }
 */
@Slf4j
public class VehicleDataAnalyzer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        OutputTag<AlarmDataModel> alarmTag = new OutputTag<>("alarms") {};

        var rawStream = env.socketTextStream("localhost", 4567);

        var vehicleStream = rawStream
                .map(VehicleDataAnalyzer::parseVehicleData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VehicleDataModel>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
                );
        final var processors = List.of(new EngineWarningProcessor(alarmTag), new HoodOpenedWarningProcessor(alarmTag), new AirbagWarningProcessor(alarmTag),
                new SpeedWarningProcessor(alarmTag), new EngineTemperatureWarningProcessor(alarmTag), new FuelLevelWarningProcessor(alarmTag));

        processors.forEach(processor -> processAndSinkAlarms(vehicleStream, processor, alarmTag));
        env.execute("Vehicle Data Analyzer");
    }

    private static void processAndSinkAlarms(SingleOutputStreamOperator<VehicleDataModel> vehicleStream, KeyedProcessFunction processor, OutputTag<AlarmDataModel> alarmTag) {
        vehicleStream
                .keyBy(VehicleDataModel::getPlate)
                .process(processor)
                .getSideOutput(alarmTag)
                .addSink(new AlarmToDatabaseSink());
    }

    private static VehicleDataModel parseVehicleData(String line) {
        try {
            return OBJECT_MAPPER.readValue(line, VehicleDataModel.class);
        } catch (Exception e) {
            log.error("Failed to parse vehicle data: " + line);
        }
        return new VehicleDataModel("unknown", false, false, false, 0, 0, 0);
    }
}