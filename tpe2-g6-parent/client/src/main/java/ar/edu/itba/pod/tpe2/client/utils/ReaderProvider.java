package ar.edu.itba.pod.tpe2.client.utils;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.core.MultiMap;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Slf4j
public class ReaderProvider {
    private final ReadService rs;
    private final DefaultReader dr;
    private final FastCsvReader fr;

    public ReaderProvider() {
        this.rs = new ReadService();
        this.dr = new DefaultReader();
        this.fr = new FastCsvReader();
    }

    public void readFilesFor(
            String city,
            boolean strictAgencies,
            boolean strictInfractions,
            MultiMap<String, Ticket> ticketsMultiMap,
            AtomicInteger key,
            String readerType
    ) throws IOException, ExecutionException, InterruptedException {
        String inPath = Validate.notBlank(System.getProperty("inPath"));
        switch (readerType.toLowerCase()) {
            case "fastcsv":
                log.info("Using FastCSV reader");
                fr.readFilesFor(city, strictAgencies, strictInfractions, ticketsMultiMap, key);
                break;
            case "parallel":
                log.info("Using parallel reader");
                rs.readFilesFor(city, strictAgencies, strictInfractions, ticketsMultiMap, key);
                break;
            default:
                log.info("Using default reader");
                dr.sequential_reader(city, strictAgencies, strictInfractions, ticketsMultiMap, key, inPath);
        }
        dr.sequential_reader(city, strictAgencies, strictInfractions, ticketsMultiMap, key, inPath);

    }

}
