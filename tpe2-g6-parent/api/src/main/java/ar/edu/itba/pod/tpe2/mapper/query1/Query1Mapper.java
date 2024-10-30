package ar.edu.itba.pod.tpe2.mapper.query1;

import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<String, Ticket, InfraAgencyPair, Long> {
    private static final Long ONE = 1L;

    @Override
    public void map(String key, Ticket value, Context<InfraAgencyPair, Long> context) {
        context.emit(new InfraAgencyPair(value.getInfraction(), value.getAgency()), ONE);
    }
}
