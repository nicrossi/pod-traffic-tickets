package ar.edu.itba.pod.tpe2.client.query.query4;

import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.query4.InfractionDiff;
import ar.edu.itba.pod.tpe2.query4.Query4Collator;
import ar.edu.itba.pod.tpe2.query4.Query4Mapper;
import ar.edu.itba.pod.tpe2.query4.Query4ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.Validate;

import java.util.*;
import java.util.concurrent.ExecutionException;

@NoArgsConstructor(force = true)
public class Query4A implements QueryStrategy {
    private static final String[] headers = { "Infraction", "Min", "Max", "Diff" };
    private final Map<String, String> args;

    public Query4A(Map<String, String> args) {
        Validate.notNull(args, "Query 4, parameter 'args' cannot be null");
        Validate.isTrue(args.containsKey("agency"), "Query 4, parameter 'agency' is required");
        Validate.isTrue(args.containsKey("n"), "Query 4, parameter 'n' is required");
        this.args = args;
    }

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog("%s INFO [main] Client - Inicio del trabajo map/reduce", mpStart);

        final String agency = args.get("agency");
        final int n = Integer.parseInt(args.get("n"));
        ICompletableFuture<Map<String, InfractionDiff>> future = job
                .mapper(new Query4Mapper(agency))
                .reducer(new Query4ReducerFactory())
                .submit();

        Map<String, InfractionDiff> results = future.get();

        Set<InfractionDiff> resultSet = new TreeSet<>(Comparator.comparing(InfractionDiff::getDiff).reversed().thenComparing(InfractionDiff::getInfraction));
        results.entrySet().stream().forEach(entry -> {resultSet.add(entry.getValue());});
        resultSet.stream().limit(n).forEach(iD -> {
            writer.addResult(
                    "%s;%d;%d;%d".formatted(iD.getInfraction(), (int) iD.getMin(), (int) iD.getMax(), (int) iD.getDiff())
            );
        });

        writer.outputResults(headers);

        Date mpEnd = new Date();
        writer.addLog("%s INFO [main] Client - Fin del trabajo map/reduce", mpEnd);
    }
}
