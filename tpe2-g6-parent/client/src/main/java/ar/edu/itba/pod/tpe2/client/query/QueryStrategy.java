package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.model.Writer;
import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Job;

import java.util.Date;
import java.util.concurrent.ExecutionException;

public
interface QueryStrategy {
    void run(Writer writer, Job<String, Ticket> job) throws ExecutionException, InterruptedException;
}