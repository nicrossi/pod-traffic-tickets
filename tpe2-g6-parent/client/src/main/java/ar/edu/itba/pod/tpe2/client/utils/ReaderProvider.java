package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.core.MultiMap;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ReaderProvider {
    public static void readFilesFor(
            String city,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key
    ) throws IOException {
        String inPath = Validate.notBlank(System.getProperty("inPath"));

        // get infractions and their names
        Map<String, String> infractionsMap = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "infractions" + city + ".csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> infractionsMap.put(parts[0], parts[1]));
        }

        // get agencies
        List<String> agencies = new ArrayList<>();
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "agencies" + city + ".csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .forEach(line -> agencies.add(line));
        }

        switch (city) {
            case "CHI":
                chiReader(inPath, agencies, infractionsMap, strictAgencies, strictInfractions, ticketsMultiMap, key);
                break;
            case "NYC":
            default:
                nycReader(inPath, agencies, infractionsMap, strictAgencies, strictInfractions, ticketsMultiMap, key);
                break;
        }
    }

    private static void chiReader(
            String inPath,
            List<String> agencies,
            Map<String, String> infractionsMap,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key
    ) throws IOException {
        // CSV File Reading and Key Value Source Loading
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "ticketsCHI.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .filter(parts -> !strictAgencies || agencies.contains(parts[2]))
                    .filter(parts -> !strictInfractions || infractionsMap.containsKey(parts[4]))
                    .map(parts -> new Ticket(
                                    parts[3],
                                    parts[2],
                                    parts[1],
                                    infractionsMap.get(parts[4]),
                                    parts[4],
                                    Double.valueOf(parts[5]),
                                    DateUtils.parseDateCHI(parts[0])
                            )
                    ).forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
    }


    private static void nycReader(
            String inPath,
            List<String> agencies,
            Map<String, String> infractionsMap,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key
    ) throws IOException {
        // CSV File Reading and Key Value Source Loading
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "ticketsNYC.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .filter(parts -> !strictAgencies || agencies.contains(parts[3]))
                    .filter(parts -> !strictInfractions || infractionsMap.containsKey(parts[1]))
                    .map(parts -> new Ticket(
                            parts[0],
                            parts[3],
                            parts[5],
                            infractionsMap.get(parts[1]),
                            parts[1],
                            Double.valueOf(parts[2]),
                            DateUtils.parseDateNYC(parts[4]))
                    ).forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
    }
}
