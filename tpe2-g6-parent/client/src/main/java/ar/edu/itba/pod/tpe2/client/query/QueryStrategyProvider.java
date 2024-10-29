package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.model.Ticket;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.Date;
import java.util.SortedSet;

class Query1 implements QueryStrategy {

    @Override
    public void run(Date timeStart, Job<String, Ticket> job) {
// TODO:
//        ICompletableFuture<SortedSet<Query1Result>> future = job
//                .mapper(new Query1Mapper())
//                .reducer(new Query1Reducer())
//                .submit();
//        SortedSet<Query1Result> results = future.get();
//        results.forEach()
//  write output file
//  log time
//  write log file
    }
}

public class QueryStrategyProvider {
    public static QueryStrategy getQueryStrategy(String queryType) {
        switch (queryType) {
            case "1":
                //TODO: do the rest
            default:
                return new Query1();
        }
    }
}
