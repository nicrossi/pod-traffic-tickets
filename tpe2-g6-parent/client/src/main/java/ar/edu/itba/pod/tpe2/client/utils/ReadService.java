package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.core.MultiMap;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.Validate;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ReadService {
    private static final String CHI = "CHI";
    private static final String NYC = "NYC";
    private final TicketParser ticketParserCHI;
    private final TicketParser ticketParserNYC;

    public ReadService() {
        this.ticketParserCHI = new TicketParserCHI();
        this.ticketParserNYC = new TicketParserNYC();
    }

    public void readFilesFor(
            String city,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key
    ) throws IOException {
        String inPath = Validate.notBlank(System.getProperty("inPath"));

        Map<String, String> infractionsMap = loadInfractions(inPath, city);
        List<String> agencies = loadAgencies(inPath, city);

        if (CHI.equalsIgnoreCase(city)) {
            processTickets(inPath + "ticketsCHI.csv", agencies, infractionsMap, strictAgencies, strictInfractions, ticketsMultiMap, key, ticketParserCHI);
        } else { // default to NYC
            processTickets(inPath + "ticketsNYC.csv", agencies, infractionsMap, strictAgencies, strictInfractions, ticketsMultiMap, key, ticketParserNYC);
        }
    }

    private static Map<String, String> loadInfractions(String inPath, String city) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inPath + "infractions" + city + ".csv"), StandardCharsets.UTF_8)) {
            return reader.lines().skip(1)
                    .map(line -> line.split(";"))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
        }
    }

    private static List<String> loadAgencies(String inPath, String city) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inPath + "agencies" + city + ".csv"), StandardCharsets.UTF_8)) {
            return reader.lines().skip(1).toList();
        }
    }

    private static void processTickets(
            String filePath,
            List<String> agencies,
            Map<String, String> infractionsMap,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key,
            TicketParser parser
    ) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            reader.lines().skip(1)
                    .parallel()
                    .map(l -> parser.parse(l, infractionsMap))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(ticket -> !strictAgencies || agencies.contains(ticket.getAgency()))
                    .filter(ticket -> !strictInfractions || ticket.getInfraction() != null)
                    .forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
        System.out.println("foo");
    }
}
