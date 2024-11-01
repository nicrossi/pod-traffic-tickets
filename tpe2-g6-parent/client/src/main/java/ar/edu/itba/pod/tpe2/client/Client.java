package ar.edu.itba.pod.tpe2.client;

import ar.edu.itba.pod.tpe2.client.utils.ReaderProvider;
import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.client.query.QueryType;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategyProvider;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static List<String> getAddresses(final String addresses) {
        return Arrays.stream(addresses.replace("'", "").split(";")).toList();
    }

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        logger.info("G6 Client Starting ...");

        final String query = Validate.notBlank(System.getProperty("query"));
        final String addresses = Validate.notBlank(System.getProperty("addresses"));
        final String city = Validate.notBlank(System.getProperty("city"));

        if(!city.matches("CHI") && !city.matches("NYC")) {
            throw new IllegalArgumentException("Invalid city: " + city);
        }

        boolean strictAgencies = false, strictInfractions = false;
        switch (query) {
            case "1":
            case "1A":
                strictAgencies = true;
                strictInfractions = true;
                break;
            case "2":
                strictAgencies = true;
                break;
            case "4":
                strictInfractions = true;
                break;
            case "3":
                strictAgencies = false;
                strictInfractions = false;
                break;
            default:
                throw new IllegalArgumentException("Invalid query: " + query);
        }

        Writer writer = new Writer();

        //getAddresses(addresses).stream().forEach(System.out::println);

        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            // add all addresses we are supposed to connect to...
            getAddresses(addresses).stream().forEach(clientNetworkConfig::addAddress);

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            // Key Value Sources
            MultiMap<String, Ticket> ticketsMultiMap = hazelcastInstance.getMultiMap("g6-tickets-" + city);
            //just se we can run queries back to back but without killing the cluster nodes each time
            ticketsMultiMap.clear();
            KeyValueSource<String, Ticket> ticketsKeyValueSource = KeyValueSource.fromMultiMap(ticketsMultiMap);

            // CSV Files Reading and Key Value Source Loading
            // [get timestamp for file reading start]
            Date tsReadStart = new Date();
            writer.addLog(
                    "%s INFO [main] Client - Inicio de la lectura del archivo",
                    tsReadStart
            );

            //idea:
            // 1. check the city selected, choose a strategy for how to read the csvs
            // 2. read through infractions and agencies csv's first to get their "full names"
            // 3. read the tickets csv, replace agency and infraction id w/ the agency name and infraction-
            //description, at no point they ask for these IDs, so maybe we can side-step having to keep
            //look-up tables for these.

            final AtomicInteger auxKey = new AtomicInteger(); //this is to be used for the tickets "uuid"
            ReaderProvider.readFilesFor(city, strictAgencies, strictInfractions, ticketsMultiMap, auxKey);

            // [get timestamp for file reading end]
            Date tsReadEnd = new Date();
            writer.addLog(
                    "%s INFO [main] Client - Fin de lectura del archivo",
                            tsReadEnd
            );


            //Job Tracker (one for each query)
            JobTracker jobTracker = hazelcastInstance.getJobTracker("g6-ticket-master-query" + query);

            //Queries

            //idea:
            // [get timestamp for mapreducer start]
            // 1. pick strategy based on query number
            // 2. do query
            // 3. print output file
            // [get timestamp for mapreducers end]
            // 4. print logging file

            Job<String, Ticket> job = jobTracker.newJob(ticketsKeyValueSource);

            QueryStrategyProvider qsp = new QueryStrategyProvider();

            QueryStrategy queryStrategy = qsp.getQueryStrategy(QueryType.selectQuery(query));
            queryStrategy.run(writer, job);

            //Results should be written by the end of querystrategy,
            //now we can output logs!
            writer.outputLogs();

        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}