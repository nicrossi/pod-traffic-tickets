package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import de.siegmar.fastcsv.reader.NamedCsvRecord;

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

    public Ticket ticketFromCsvRecord(final NamedCsvRecord r, Map<String, String> infractionsMap) {
        return new Ticket(
                r.getField("Plate"),
                r.getField("Issuing Agency"),
                r.getField("County Name"),
                infractionsMap.get(r.getField("Infraction ID")),
                r.getField("Infraction ID"),
                Double.parseDouble(r.getField("Fine Amount")),
                DateUtils.parseDateNYC(r.getField("Issue Date")));
    }
}
