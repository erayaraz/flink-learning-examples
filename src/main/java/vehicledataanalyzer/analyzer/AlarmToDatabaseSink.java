package vehicledataanalyzer.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import vehicledataanalyzer.model.vehicle.AlarmDataModel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;

@Slf4j
public class AlarmToDatabaseSink implements SinkFunction<AlarmDataModel> {
    @Override
    public void invoke(AlarmDataModel alarmDataModel, Context context) throws Exception {
        String insertSQL = "INSERT INTO alarms (plate, alarm_type,created_at) VALUES (?, ?, ?)";
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/flinkdb", "postgres", "postgres");
             var statement = connection.prepareStatement(insertSQL)) {
            statement.setString(1, alarmDataModel.getPlate());
            statement.setString(2, alarmDataModel.getAlarmType());
            statement.setTimestamp(3, new Timestamp(alarmDataModel.getTimestamp()));
            statement.executeUpdate();
        } catch (Exception e) {
            log.error("Failed to insert alarm data into database: " + alarmDataModel, e);
        }
    }
}