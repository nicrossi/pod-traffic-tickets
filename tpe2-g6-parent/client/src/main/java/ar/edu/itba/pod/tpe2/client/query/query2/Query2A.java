package ar.edu.itba.pod.tpe2.client.query.query2;

import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.query2.Query2Collator;
import ar.edu.itba.pod.tpe2.query2.Query2Mapper;
import ar.edu.itba.pod.tpe2.query2.Query2ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;


@Slf4j
@NoArgsConstructor(force = true)
public class Query2A implements QueryStrategy {
    private static final String[] headers = {"Agency", "Year", "Month", "YTD"};

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog("%s INFO [main] Client - Inicio del trabajo map/reduce", mpStart);

        ICompletableFuture<Map<String, Double>> future = job
                .mapper(new Query2Mapper())
                .reducer(new Query2ReducerFactory())
                .submit();

        Map<String, Double> results = future.get();

        Map<String, Map<String, Double[]>> agencyRevenueMap = new TreeMap<>();

        results.entrySet().stream()
                .forEach(entry -> {
                    String[] parts = getParts(entry.getKey());
                    String agency = parts[0];
                    String year = parts[1];
                    String month = parts[2];
                    double amount = entry.getValue();

                    agencyRevenueMap.computeIfAbsent(agency, k -> new TreeMap<>())
                            .computeIfAbsent(year, k -> new Double[12])[Integer.parseInt(month) - 1] = amount;
                });

        computeYTD(agencyRevenueMap).stream().forEach(writer::addResult);

        writer.outputResults(headers);

        Date mpEnd = new Date();
        writer.addLog("%s INFO [main] Client - Fin del trabajo map/reduce", mpEnd);
    }

    private String[] getParts(String key) {
        String[] parts = key.split(":");
        //0 is agency, 1 is year, 2 is month
        return parts;
    }

    private List<String> computeYTD(Map<String, Map<String, Double[]>> agencyRevenueMap) {
        List<String> results = new ArrayList<>();
        for (Map.Entry<String, Map<String, Double[]>> agencyEntry : agencyRevenueMap.entrySet()) {
            String agency = agencyEntry.getKey();
            // key = YYYY, value = [jan-revenue, feb-revenue, ..., dec-revenue]
            Map<String, Double[]> yearMonthMap = agencyEntry.getValue();
            for (Map.Entry<String, Double[]> yearEntry : yearMonthMap.entrySet()) {
                String year = yearEntry.getKey();
                Double[] monthArray = yearEntry.getValue();
                double ytd = 0;
                for (int i = 0; i < monthArray.length; i++) {
                    if (monthArray[i] == null) {
                        continue;
                    }
                    ytd += monthArray[i];
                    results.add(agency + ";" + year + ";" + (i + 1) + ";" + (int) ytd);
                }
            }
        }
        return results;
    }
}
