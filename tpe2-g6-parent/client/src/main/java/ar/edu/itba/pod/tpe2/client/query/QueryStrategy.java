package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.model.Ticket;
import com.hazelcast.mapreduce.Job;

import java.util.Date;

public
interface QueryStrategy {
    void run(Date timeStart, Job<String, Ticket> job);
}