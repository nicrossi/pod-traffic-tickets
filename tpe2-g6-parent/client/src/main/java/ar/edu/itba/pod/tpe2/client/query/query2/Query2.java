package ar.edu.itba.pod.tpe2.client.query.query2;

import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Job;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ExecutionException;


@Slf4j
@NoArgsConstructor(force = true)
public class Query2 implements QueryStrategy {

    @Override
    public void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException {

    }
}
