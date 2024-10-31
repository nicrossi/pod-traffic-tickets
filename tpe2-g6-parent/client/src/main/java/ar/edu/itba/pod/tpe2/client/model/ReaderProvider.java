package ar.edu.itba.pod.tpe2.client.model;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

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
    public static void readFilesFor(String city, boolean strictAgencies, boolean strictInfractions, MultiMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        switch (city) {
            case "CHI":
                chiReader(strictAgencies, strictInfractions, ticketsMultiMap, key);
                break;
            case "NYC":
            default:
                nycReader(strictAgencies, strictInfractions, ticketsMultiMap, key);
                break;
        }
    }

    private static void chiReader(boolean strictAgencies, boolean strictInfractions, MultiMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        String inPath = System.getProperty("inPath");

        // get infractions and their names
        Map<String, String> infractionsMap = new HashMap<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath + "/infractionsCHI.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> infractionsMap.put(parts[0], parts[1]));
        }

        // get agencies
        List<String> agencies = new ArrayList<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath + "/agenciesCHI.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .forEach(line -> agencies.add(line));
        }

        // CSV File Reading and Key Value Source Loading
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/ticketsCHI.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .filter(parts -> !strictAgencies || agencies.contains(parts[2]))
                    .filter(parts -> !strictInfractions || infractionsMap.containsKey(parts[4]))
                    .map(parts -> new Ticket(
                            parts[3],
                            parts[2],
                            parts[1],
                            infractionsMap.get(parts[4]),
                            Double.valueOf(parts[5]),
                            LocalDateTime.parse(parts[0], formatter)
                            )
                    ).forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
    }


    private static void nycReader(boolean strictAgencies, boolean strictInfractions, MultiMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        String inPath = System.getProperty("inPath");

        // get infractions and their names
        Map<String, String> infractionsMap = new HashMap<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath + "/infractionsNYC.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> infractionsMap.put(parts[0], parts[1]));
        }

        // get agencies
        List<String> agencies = new ArrayList<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath + "/agenciesNYC.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .forEach(line -> agencies.add(line));
        }

        // CSV File Reading and Key Value Source Loading
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/ticketsNYC.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .filter(parts -> !strictAgencies || agencies.contains(parts[3]))
                    .filter(parts -> !strictInfractions || infractionsMap.containsKey(parts[1]))
                    .map(parts -> new Ticket(
                            parts[0],
                            parts[3],
                            parts[5],
                            infractionsMap.get(parts[1]),
                            Double.valueOf(parts[2]),
                            LocalDateTime.parse(parts[4], formatter))
                    ).forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
    }
}
