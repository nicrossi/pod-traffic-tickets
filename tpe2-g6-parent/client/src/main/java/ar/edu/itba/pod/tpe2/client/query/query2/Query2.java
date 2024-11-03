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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@Slf4j
@NoArgsConstructor(force = true)
public class Query2 implements QueryStrategy {
    private static final String[] headers = { "Agency", "Year", "Month", "YTD" };

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog("%s INFO [main] Client - Inicio del trabajo map/reduce", mpStart);

        ICompletableFuture<List<String>> future = job
                .mapper(new Query2Mapper())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator());

        List<String> results = future.get();
        results.forEach(writer::addResult);

        writer.outputResults(headers);

        Date mpEnd = new Date();
        writer.addLog("%s INFO [main] Client - Fin del trabajo map/reduce", mpEnd);
    }
}
