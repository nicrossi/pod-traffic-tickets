package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.core.MultiMap;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class FastCsvReader {
    private static final String CHI = "CHI";
    private static final String NYC = "NYC";
    private final TicketParser ticketParserCHI;
    private final TicketParser ticketParserNYC;
    private final ExecutorService executor;

    public FastCsvReader() {
        this.ticketParserCHI = new TicketParserCHI();
        this.ticketParserNYC = new TicketParserNYC();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void readFilesFor(
            String city,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key
    ) throws ExecutionException, InterruptedException {
        String inPath = Validate.notBlank(System.getProperty("inPath"));

        CompletableFuture<Map<String, String>> infractionsFuture = getInfractionsFuture(city, inPath);
        CompletableFuture<List<String>> agenciesFuture = getAgenciesFuture(city, inPath);

        CompletableFuture.allOf(infractionsFuture, agenciesFuture).thenRun(() -> {
            try {
                Map<String, String> infractionsMap = infractionsFuture.get();
                List<String> agencies = agenciesFuture.get();
                if (CHI.equalsIgnoreCase(city)) {
                    processTickets(inPath + "ticketsCHI.csv", agencies, infractionsMap, strictAgencies,
                            strictInfractions, ticketsMultiMap, key, ticketParserCHI);
                } else {
                    processTickets(inPath + "ticketsNYC.csv", agencies, infractionsMap, strictAgencies,
                            strictInfractions, ticketsMultiMap, key, ticketParserNYC);
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                log.error("[ReadService] Error reading input files.", e);
            }
        }).get();

        executor.shutdown();
    }

    private CompletableFuture<List<String>> getAgenciesFuture(String city, String inPath) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return loadAgencies(inPath, city);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    private CompletableFuture<Map<String, String>> getInfractionsFuture(String city, String inPath) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return loadInfractions(inPath, city);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    private static Map<String, String> loadInfractions(String inPath, String city) throws IOException {
        Path infractionsPath = Paths.get(inPath + "infractions" + city + ".csv");
        validateIfFileExist(infractionsPath);
        Reader reader = Files.newBufferedReader(infractionsPath, StandardCharsets.UTF_8);
        try (CsvReader<NamedCsvRecord> csvreader = CsvReader.builder()
                .skipEmptyLines(true)
                .fieldSeparator(';')
                .ofNamedCsvRecord(reader) ) {
            return csvreader.stream()
                    .skip(1)
                    .collect(Collectors.toMap(row -> row.getField(0), row -> row.getField(1)));
        }
    }

    private static List<String> loadAgencies(String inPath, String city) throws IOException {
        Path agenciesPath = Paths.get(inPath + "agencies" + city + ".csv");
        validateIfFileExist(agenciesPath);
        Reader reader = Files.newBufferedReader(agenciesPath, StandardCharsets.UTF_8);
        try (CsvReader<NamedCsvRecord> csvreader = CsvReader.builder()
                .skipEmptyLines(true)
                .fieldSeparator(';')
                .ofNamedCsvRecord(reader) ) {
            return csvreader.stream()
                    .skip(1)
                    .map(row -> row.getField(0))
                    .collect(Collectors.toList());
        }
    }

    private void processTickets(
            String filePath,
            List<String> agencies,
            Map<String, String> infractionsMap,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key,
            TicketParser parser
    ) throws IOException {
        Path inputFilePath = Paths.get(filePath);
        validateIfFileExist(inputFilePath);
        Reader reader = Files.newBufferedReader(inputFilePath, StandardCharsets.UTF_8);
        try (CsvReader<NamedCsvRecord> csvreader = CsvReader.builder()
                .skipEmptyLines(true)
                .fieldSeparator(';')
                .ofNamedCsvRecord(reader)) {
            csvreader.stream()
                    .skip(1)
                    .forEach(row -> {
                        Ticket t = parser.ticketFromCsvRecord(row, infractionsMap);
                        if ((!strictAgencies || agencies.contains(t.getAgency())) &&
                                (!strictInfractions || t.getInfraction() != null)) {
                            ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), t);
                        }
                    });
        }
    }

    private void processBatch(
            List<String> batch,
            List<String> agencies,
            Map<String, String> infractionsMap,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key,
            TicketParser parser
    ) {
        List<CompletableFuture<Void>> futures = batch.stream()
                .map(line -> CompletableFuture.runAsync(() -> {
                    Optional<Ticket> optionalTicket = parser.parse(line, infractionsMap);
                    if (optionalTicket.isPresent()) {
                        Ticket ticket = optionalTicket.get();
                        if ((!strictAgencies || agencies.contains(ticket.getAgency())) &&
                                (!strictInfractions || ticket.getInfraction() != null)) {
                            ticketsMultiMap.put(String.valueOf(key.getAndIncrement()), ticket);
                        }
                    }
                }, executor))
                .toList();

        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private static void validateIfFileExist(Path infractionsPath) throws FileNotFoundException {
        if (!Files.exists(infractionsPath)) {
            throw new FileNotFoundException("File not found: " + infractionsPath);
        }
    }
}
