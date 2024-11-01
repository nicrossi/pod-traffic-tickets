package ar.edu.itba.pod.tpe2.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class Ticket implements DataSerializable {
    private String plate, agency, county, infraction;
    private Double amount;
    private LocalDateTime date;

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(plate);
        objectDataOutput.writeUTF(agency);
        objectDataOutput.writeUTF(county);
        objectDataOutput.writeUTF(infraction);
        objectDataOutput.writeDouble(amount);
        objectDataOutput.writeObject(date);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        plate = objectDataInput.readUTF();
        agency = objectDataInput.readUTF();
        county = objectDataInput.readUTF();
        infraction = objectDataInput.readUTF();
        amount = objectDataInput.readDouble();
        date = objectDataInput.readObject();
    }
}
