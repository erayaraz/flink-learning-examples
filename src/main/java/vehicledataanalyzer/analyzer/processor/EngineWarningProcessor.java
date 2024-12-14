package vehicledataanalyzer.analyzer.processor;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import vehicledataanalyzer.model.vehicle.VehicleDataModel;

import java.time.Duration;

public class EngineWarningProcessor extends KeyedProcessFunction<String, VehicleDataModel, String> {
    private static final long ALARM_COOLDOWN = Duration.ofMinutes(1).toMillis();
    private static final int WARNING_THRESHOLD = 3;

    private transient ValueState<Integer> consecutiveWarningCount;
    private transient ValueState<Long> lastAlarmTimestamp;

    @Override
    public void open(final Configuration parameters) {
        consecutiveWarningCount = getRuntimeContext().getState(new ValueStateDescriptor<>("warningCount", Integer.class));
        lastAlarmTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAlarmTimestamp", Long.class));
    }

    @Override
    public void processElement(VehicleDataModel vehicle, Context ctx, Collector<String> out) throws Exception {
        var count = consecutiveWarningCount.value() == null ? 0 : consecutiveWarningCount.value();
        var lastAlarmTime = lastAlarmTimestamp.value() == null ? 0L : lastAlarmTimestamp.value();
        if (vehicle.isEngineWarning()) {
            count++;
            if (count >= WARNING_THRESHOLD) {
                var currentTime = ctx.timerService().currentProcessingTime();
                if (currentTime - lastAlarmTime >= ALARM_COOLDOWN) {
                    out.collect("ALARM! Plate: " + vehicle.getPlate() + " - Engine warning detected!");
                    lastAlarmTimestamp.update(currentTime);
                }
                count = 0;
            }
        } else {
            count = 0;
        }
        consecutiveWarningCount.update(count);
    }
}