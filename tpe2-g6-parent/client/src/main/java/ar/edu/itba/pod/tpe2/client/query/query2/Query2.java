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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@Slf4j
@NoArgsConstructor(force = true)
public class Query2 implements QueryStrategy {

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        ICompletableFuture<List<String>> future = job
                .mapper(new Query2Mapper())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator());

        List<String> results = future.get();
        results.forEach(System.out::println);

        // TODO: Add writer
    }
}
