package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.common.Ticket;
import com.hazelcast.mapreduce.Job;

import java.util.Date;
import java.util.concurrent.ExecutionException;

public
interface QueryStrategy {
    void run(Date timeStart, Job<String, Ticket> job) throws ExecutionException, InterruptedException;
}