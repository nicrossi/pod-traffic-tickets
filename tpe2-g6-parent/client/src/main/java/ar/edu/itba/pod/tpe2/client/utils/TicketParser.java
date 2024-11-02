package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;

import java.util.Map;
import java.util.Optional;

@FunctionalInterface
public interface TicketParser {
    Optional<Ticket> parse(String line, Map<String, String> infractionsMap);
}