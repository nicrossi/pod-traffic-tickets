package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import de.siegmar.fastcsv.reader.NamedCsvRecord;

import java.util.Map;
import java.util.Optional;

public class TicketParserCHI implements TicketParser {
    @Override
    public Optional<Ticket> parse(String line, Map<String, String> infractionsMap) {
        String[] parts = line.split(";");
        if (parts.length < 6) return Optional.empty();
        return Optional.of(new Ticket(
                parts[3],
                parts[2],
                parts[1],
                infractionsMap.get(parts[4]),
                parts[4],
                Double.valueOf(parts[5]),
                DateUtils.parseDateCHI(parts[0])
        ));
    }

    public Ticket ticketFromCsvRecord(final NamedCsvRecord r, Map<String, String> infractionsMap) {
        return new Ticket(
                r.getField("license_plate_number"),
                r.getField("unit_description"),
                r.getField("community_area_name"),
                infractionsMap.get(r.getField("violation_code")),
                r.getField("violation_code"),
                Double.parseDouble(r.getField("fine_amount")),
                DateUtils.parseDateCHI(r.getField("issue_date")));
    }
}
