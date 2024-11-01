package ar.edu.itba.pod.tpe2.client.query.query1;

import ar.edu.itba.pod.tpe2.client.model.Writer;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import ar.edu.itba.pod.tpe2.mapper.query1.Query1Mapper;
import ar.edu.itba.pod.tpe2.reducer.query1.Query1ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import lombok.NoArgsConstructor;

import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@NoArgsConstructor
public class Query1 implements QueryStrategy {

    private static final String[] headers = {"Infraction", "Agency", "Tickets"};

    public Query1(String s) {
    }

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog(
                "%s INFO [main] Client - Inicio del trabajo map/reduce",
                mpStart
        );

        // TODO:
        //ICompletableFuture<SortedSet<Query1Result>>
        ICompletableFuture<Map<InfraAgencyPair, Long>> future = job
                .mapper(new Query1Mapper())
                .reducer(new Query1ReducerFactory())
                // - ordering by infraction and then by agency
                // can be done in a collator :)
                .submit();

        // Wait and retrieve the result
        //TODO: use SortedSet<Query1Result> when collator is done
        Map<InfraAgencyPair, Long> results = future.get();

        results.entrySet().stream()
                //we could do the sorting in a collator...
                .sorted(
                        Map.Entry.<InfraAgencyPair, Long>comparingByValue().reversed()
                                .thenComparing(
                                        Comparator.comparing(o -> o.getKey().getAgency())
                                )
                )
                //we could repackage the results into Query1Result in a collator...
                .forEach(entry -> {
                    InfraAgencyPair pair = entry.getKey();
                    //TODO: print to output file
                    writer.addResult("%s;%s;%d".formatted(pair.getInfraction(), pair.getAgency(), entry.getValue()));
                });
        //Write out results file
        writer.outputResults(headers);

        //results.forEach(System.out::println)
        //  write output file
        //  log time
        //  write log file
        Date mpEnd = new Date();
        writer.addLog(
                "%s INFO [main] Client - Fin del trabajo map/reduce",
                mpEnd
        );
    }
}