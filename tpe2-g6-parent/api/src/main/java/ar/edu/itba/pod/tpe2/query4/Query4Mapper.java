package ar.edu.itba.pod.tpe2.query4;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.apache.commons.lang3.Validate;

public class Query4Mapper implements Mapper<String, Ticket, String, Double> {
    private final String agency;

    public Query4Mapper(final String agency) {
        Validate.notBlank(agency, "Agency cannot be blank");
        this.agency = agency.replaceAll("_", " ").toLowerCase();
    }

    @Override
    public void map(String key, Ticket ticket, Context<String, Double> context) {
        if (agency.equals(ticket.getAgency().toLowerCase())) {
            String agencyInfractionKey = String.format("%s:%s", ticket.getAgency(), ticket.getInfraction());
            context.emit(agencyInfractionKey, ticket.getAmount());
        }
    }
}
