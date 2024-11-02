package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;

import java.util.Map;
import java.util.Optional;

public class TicketParserNYC implements TicketParser {
    @Override
    public Optional<Ticket> parse(String line, Map<String, String> infractionsMap)  {
        String[] parts = line.split(";");
        if (parts.length < 6) return Optional.empty();
        return Optional.of(new Ticket(
                parts[0],
                parts[3],
                parts[5],
                infractionsMap.get(parts[1]),
                parts[1],
                Double.valueOf(parts[2]),
                DateUtils.parseDateNYC(parts[4])
        ));
    }
}
