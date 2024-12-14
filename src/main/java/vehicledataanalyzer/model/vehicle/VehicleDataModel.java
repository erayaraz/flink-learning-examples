package vehicledataanalyzer.model.vehicle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VehicleDataModel {
    private String plate;
    private boolean engineWarning;
    private boolean airbagWarning;
    private boolean hoodOpen;
    private int speed;
    private int engineTemperature;
    private int fuelLevel;
}