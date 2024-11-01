package ar.edu.itba.pod.tpe2.query2;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query2Mapper implements Mapper<String, Ticket, String, Double> {

    @Override
    public void map(String key, Ticket ticket, Context<String, Double> context) {
        String agency = ticket.getAgency();
        LocalDateTime date = ticket.getDate();
        String yearMonth = String.format("%s:%s", date.getYear(), date.getMonth().getValue());
        context.emit(String.format("%s:%s", agency, yearMonth), ticket.getAmount());
    }
}
