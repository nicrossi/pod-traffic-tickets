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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ReaderProvider {
    public static void readFilesFor(String city, MultiMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        switch (city) {
            case "CHI":
                chiReader(ticketsMultiMap, key);
                break;
            case "NYC":
            default:
                nycReader(ticketsMultiMap, key);
                break;
        }
    }

    private static void chiReader(MultiMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        String inPath = System.getProperty("inPath");

        // get infractions and their names
        Map<String, String> infractionsMap = new HashMap<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath + "/infractionsCHI.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> infractionsMap.put(parts[0], parts[1]));
        }

        // CSV File Reading and Key Value Source Loading
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/ticketsCHI.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .map(line -> new Ticket(
                            line[3],
                            line[2],
                            line[1],
                            infractionsMap.get(line[4]),
                            Double.valueOf(line[5]),
                            LocalDateTime.parse(line[0], formatter)
                            )
                    ).forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
    }


    private static void nycReader(MultiMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        String inPath = System.getProperty("inPath");

        // get infractions and their names
        Map<String, String> infractionsMap = new HashMap<>();
        try(Stream<String> lines = Files.lines(Paths.get(inPath + "/infractionsNYC.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(parts -> infractionsMap.put(parts[0], parts[1]));
        }

        // CSV File Reading and Key Value Source Loading
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try (Stream<String> lines = Files.lines(Paths.get(inPath + "/ticketsNYC.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .map(line -> new Ticket(line[0],
                            line[3],
                            line[5],
                            infractionsMap.get(line[1]), //check if this fails tho
                            Double.valueOf(line[2]),
                            LocalDateTime.parse(line[4], formatter))
                    ).forEach(ticket -> ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket));
        }
    }
}
