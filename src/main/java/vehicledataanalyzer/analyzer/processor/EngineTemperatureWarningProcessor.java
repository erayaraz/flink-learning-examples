package vehicledataanalyzer.analyzer.processor;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import vehicledataanalyzer.model.vehicle.VehicleDataModel;

import java.time.Duration;

public class EngineTemperatureWarningProcessor extends KeyedProcessFunction<String, VehicleDataModel, String> {
    private static final long ALARM_COOLDOWN = Duration.ofMinutes(3).toMillis();
    private static final int TEMPERATURE_THRESHOLD = 80;

    private transient ValueState<Long> lastAlarmTimestamp;

    @Override
    public void open(final Configuration parameters) {
        lastAlarmTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAlarmTimestamp", Long.class));
    }

    @Override
    public void processElement(VehicleDataModel vehicle, Context ctx, Collector<String> out) throws Exception {
        var lastAlarmTime = lastAlarmTimestamp.value() == null ? 0L : lastAlarmTimestamp.value();
        var currentTime = ctx.timerService().currentProcessingTime();

        if (vehicle.getEngineTemperature() > TEMPERATURE_THRESHOLD) {
            if (currentTime - lastAlarmTime >= ALARM_COOLDOWN) {
                out.collect("ALARM! Plate: " + vehicle.getPlate() + " - Engine temperature exceeds 80 degrees!");
                lastAlarmTimestamp.update(currentTime);
            }
        }
    }
}