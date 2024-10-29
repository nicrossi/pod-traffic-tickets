package ar.edu.itba.pod.tpe2.client.model;

import com.hazelcast.core.IMap;
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
    public static void readFilesFor(String city, IMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
        switch (city) {
            //TODO: impl CHI
            case "NYC":
            default:
                nycReader(ticketsMultiMap, key);
        }
    }

    private static void nycReader(IMap<String, Ticket> ticketsMultiMap, AtomicInteger key) throws IOException {
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
