package ar.edu.itba.pod.tpe2.query4;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class InfractionDiff implements DataSerializable {
    private double min;
    private double max;
    private double diff;
    private String infraction;

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeDouble(min);
        objectDataOutput.writeDouble(max);
        objectDataOutput.writeDouble(diff);
        objectDataOutput.writeUTF(infraction);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        min = objectDataInput.readDouble();
        max = objectDataInput.readDouble();
        diff = objectDataInput.readDouble();
        infraction = objectDataInput.readUTF();
    }
}
