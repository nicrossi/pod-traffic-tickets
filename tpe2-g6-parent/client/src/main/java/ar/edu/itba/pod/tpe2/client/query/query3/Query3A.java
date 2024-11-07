package ar.edu.itba.pod.tpe2.client.query.query3;

import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.utils.DateUtils;
import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.query3.Query3Collator;
import ar.edu.itba.pod.tpe2.query3.Query3Mapper;
import ar.edu.itba.pod.tpe2.query3.Query3ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor(force = true)
public class Query3A implements QueryStrategy {
    private static final String[] headers = {"County", "Percentage"};
    private final Map<String, String> args;

    public Query3A(Map<String, String> args) {
        Validate.notNull(args, "Query 3, parameter 'args' cannot be null");
        Validate.isTrue(args.containsKey("from"), "Query 3, parameter 'from' is required");
        Validate.isTrue(args.containsKey("to"), "Query 3, parameter 'to' is required");
        Validate.isTrue(args.containsKey("n"), "Query 3, parameter 'n' is required");
        this.args = args;
    }

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog("%s INFO [main] Client - Inicio del trabajo map/reduce", mpStart);

        LocalDateTime from = DateUtils.parseDate(args.get("from"));
        LocalDateTime to = DateUtils.parseDate(args.get("to")).withHour(23).withMinute(59).withSecond(59);
        int n = Integer.parseInt(args.get("n"));
        ICompletableFuture<Map<String, Map<String, Long>>> future = job
                .mapper(new Query3Mapper(from, to))
                .reducer(new Query3ReducerFactory(n))
                .submit();

        Map<String, Map<String, Long>> results = future.get();
        Map<String, Map<String, Long>> countyAggregates = aggregateDataByCounty(results.entrySet());
        Map<String, Double> countyPercentages = calculatePercentages(countyAggregates);

        countyPercentages.entrySet().stream()
                .filter(e -> e.getValue() != 0)
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed()
                        .thenComparing(
                                Comparator.comparing(Map.Entry::getKey)
                        )
                )
                .forEach(e -> {
                    writer.addResult("%s;%.2f%%".formatted(e.getKey(), truncate2Decimals(e.getValue())));
                });

        writer.outputResults(headers);

        Date mpEnd = new Date();
        writer.addLog("%s INFO [main] Client - Fin del trabajo map/reduce", mpEnd);

    }

    private double truncate2Decimals(double number) {
        Validate.isTrue(number >= 0, "Number must be positive");
        return Math.floor(number * 100) / 100;
    }

    private Map<String, Map<String, Long>> aggregateDataByCounty(Iterable<Map.Entry<String, Map<String, Long>>> values) {
        Map<String, Map<String, Long>> countyAggregates = new HashMap<>();

        values.forEach(entry -> {
            String county = entry.getKey().split(":")[0];
            Map<String, Long> stats = entry.getValue();

            countyAggregates.computeIfAbsent(county, k -> new HashMap<>())
                    .merge("repeatOffenderCount", stats.get("repeatOffenderCount"), Long::sum);
            countyAggregates.get(county)
                    .merge("uniquePlateCount", stats.get("uniquePlateCount"), Long::sum);
        });

        return countyAggregates;
    }

    private Map<String, Double> calculatePercentages(Map<String, Map<String, Long>> countyAggregates) {
        return countyAggregates.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> (double) entry.getValue()
                                .get("repeatOffenderCount") / entry.getValue()
                                .get("uniquePlateCount") * 100
                ));
    }
}
