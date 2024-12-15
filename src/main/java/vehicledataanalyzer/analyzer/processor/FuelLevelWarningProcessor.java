package vehicledataanalyzer.analyzer.processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import vehicledataanalyzer.analyzer.enums.AlarmType;
import vehicledataanalyzer.model.vehicle.AlarmDataModel;
import vehicledataanalyzer.model.vehicle.VehicleDataModel;

import java.time.Duration;

public class FuelLevelWarningProcessor extends KeyedProcessFunction<String, VehicleDataModel, String> {
    private static final long ALARM_COOLDOWN = Duration.ofMinutes(5).toMillis();
    private static final int FUEL_LEVEL_THRESHOLD = 5;
    private transient ValueState<Long> lastAlarmTimestamp;
    private final OutputTag<AlarmDataModel> alarmOutputTag;

    public FuelLevelWarningProcessor(OutputTag<AlarmDataModel> alarmOutputTag) {
        this.alarmOutputTag = alarmOutputTag;
    }

    @Override
    public void open(final Configuration parameters) {
        lastAlarmTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAlarmTimestamp", Long.class));
    }

    @Override
    public void processElement(VehicleDataModel vehicle, Context ctx, Collector<String> out) throws Exception {
        var lastAlarmTime = lastAlarmTimestamp.value() == null ? 0L : lastAlarmTimestamp.value();
        var currentTime = ctx.timerService().currentProcessingTime();
        if (vehicle.getFuelLevel() > 0 && vehicle.getFuelLevel() < FUEL_LEVEL_THRESHOLD && currentTime - lastAlarmTime >= ALARM_COOLDOWN) {
            out.collect("ALARM! Plate: " + vehicle.getPlate() + " - Fuel level below 5!");
            ctx.output(alarmOutputTag, new AlarmDataModel(vehicle.getPlate(), AlarmType.FUEL_LEVEL_LOW.toString(), currentTime));
            lastAlarmTimestamp.update(currentTime);
        }
    }
}