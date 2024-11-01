package ar.edu.itba.pod.tpe2.query3;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query3Mapper implements Mapper<String, Ticket, String, String> {
    private static final String SEPARATOR = ":";
    private final LocalDateTime from;
    private final LocalDateTime to;

    public Query3Mapper(LocalDateTime from, LocalDateTime to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public void map(String key, Ticket value, Context<String, String> context) {
        if (value.getDate().isAfter(from) && value.getDate().isBefore(to)) {
            String countyInfractionKey = String.format("%s:%s", value.getCounty(), value.getInfraction());
            context.emit(countyInfractionKey, value.getPlate());
        }
    }
}
