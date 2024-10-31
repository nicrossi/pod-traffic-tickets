package ar.edu.itba.pod.tpe2.client.query.query1;

import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import ar.edu.itba.pod.tpe2.mapper.query1.Query1Mapper;
import ar.edu.itba.pod.tpe2.reducer.query1.Query1ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query1 implements QueryStrategy {

    @Override
    public void run(Date timeStart, Job<String, Ticket> job) throws ExecutionException, InterruptedException {
// TODO:
        //ICompletableFuture<SortedSet<Query1Result>>
        ICompletableFuture<Map<InfraAgencyPair, Long>> future = job
                .mapper(new Query1Mapper())
                .reducer(new Query1ReducerFactory())
                .submit();

        //What's missing?
        // - ordering by infraction and then by agency
        // that can be done in a collator :)

        //SortedSet<Query1Result>
        // Wait and retrieve the result
        Map<InfraAgencyPair, Long> results  = future.get();

        results.entrySet().stream().forEach(entry -> {
            InfraAgencyPair pair = entry.getKey();
            System.out.println("%s;%s;%d".formatted(pair.getInfraction(), pair.getAgency(), entry.getValue()));
        });
        Date timeEnd = new Date();
        System.out.println("%s INFO [main] Client - Inicio del trabajo map/reduce".formatted(timeStart.toString()));
        System.out.println("%s INFO [main] Client - Fin del trabajo map/reduce".formatted(timeEnd.toString()));
        //results.forEach(System.out::println)
//  write output file
//  log time
//  write log file
    }
}