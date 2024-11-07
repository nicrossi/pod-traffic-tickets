package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import de.siegmar.fastcsv.reader.NamedCsvRecord;

import java.util.Map;
import java.util.Optional;

public interface TicketParser {
    Optional<Ticket> parse(String line, Map<String, String> infractionsMap);

    Ticket ticketFromCsvRecord(NamedCsvRecord r, Map<String, String> infractionsMap);
}