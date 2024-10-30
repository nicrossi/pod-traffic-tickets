package ar.edu.itba.pod.tpe2.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class Ticket implements DataSerializable {
    private String plate, agency, county, infraction;
    private Double amount;
    private LocalDateTime date;

    public Ticket(String plate,
                  String agency,
                  String county,
                  String infraction,
                  Double amount,
                  LocalDateTime date) {
        this.plate = plate;
        this.agency = agency;
        this.county = county;
        this.infraction = infraction;
        this.amount = amount;
        this.date = date;
    }

    public String getInfraction() {
        return infraction;
    }
    public String getAgency() {
        return agency;
    }

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
