package ar.edu.itba.pod.tpe2.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class InfraAgencyPair implements DataSerializable {
    private String infraction, agency;

    @Override
    public int hashCode() {
        return infraction.hashCode() + agency.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof InfraAgencyPair) {
            InfraAgencyPair pair = (InfraAgencyPair) obj;
            return infraction.equals(pair.infraction) && agency.equals(pair.agency);
        }
        return false;
    }

    public InfraAgencyPair(String infraction, String agency) {
        this.infraction = infraction;
        this.agency = agency;
    }

    public InfraAgencyPair() {

    }

    public String getInfraction() {
        return infraction;
    }
    public String getAgency() {
        return agency;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(infraction);
        objectDataOutput.writeUTF(agency);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        infraction = objectDataInput.readUTF();
        agency = objectDataInput.readUTF();
    }
}
