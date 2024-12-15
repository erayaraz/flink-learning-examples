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

public class SpeedWarningProcessor extends KeyedProcessFunction<String, VehicleDataModel, String> {
    private static final long ALARM_COOLDOWN = Duration.ofMinutes(1).toMillis();
    private static final int SPEED_THRESHOLD = 130;
    private transient ValueState<Long> lastAlarmTimestamp;
    private final OutputTag<AlarmDataModel> alarmOutputTag;

    public SpeedWarningProcessor(OutputTag<AlarmDataModel> alarmOutputTag) {
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
        if (vehicle.getSpeed() > SPEED_THRESHOLD && currentTime - lastAlarmTime >= ALARM_COOLDOWN) {
            out.collect("ALARM! Plate: " + vehicle.getPlate() + " - Speed exceeds 130!");
            ctx.output(alarmOutputTag, AlarmDataModel.builder().plate((vehicle.getPlate())).alarmType(AlarmType.SPEED_EXCEEDED.toString()).timestamp(currentTime).build());
            lastAlarmTimestamp.update(currentTime);
        }
    }
}