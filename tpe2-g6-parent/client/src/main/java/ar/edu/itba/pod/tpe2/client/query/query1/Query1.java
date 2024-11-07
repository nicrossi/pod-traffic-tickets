package ar.edu.itba.pod.tpe2.client.query.query1;

import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.common.query1.Query1Collator;
import ar.edu.itba.pod.tpe2.common.query1.Query1Mapper;
import ar.edu.itba.pod.tpe2.common.query1.Query1ReducerFactory;
import ar.edu.itba.pod.tpe2.common.query1.Query1Result;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

@NoArgsConstructor
public class Query1 implements QueryStrategy {

    private static final String[] headers = {"Infraction", "Agency", "Tickets"};

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog(
                "%s INFO [main] Client - Inicio del trabajo map/reduce",
                mpStart
        );

        // TODO:
        ICompletableFuture<SortedSet<Query1Result>> future = job
                .mapper(new Query1Mapper())
                .reducer(new Query1ReducerFactory())
                .submit(new Query1Collator());

        // Wait and retrieve the result
        SortedSet<Query1Result> results = future.get();
        results.forEach(query1Result -> {
            writer.addResult(query1Result.toString());
        });

        //Write out results file
        writer.outputResults(headers);

        Date mpEnd = new Date();
        writer.addLog(
                "%s INFO [main] Client - Fin del trabajo map/reduce",
                mpEnd
        );
    }
}