package vehicledataanalyzer.model.vehicle;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AlarmDataModel {
    private String plate;
    private String alarmType;
    private long timestamp;
}