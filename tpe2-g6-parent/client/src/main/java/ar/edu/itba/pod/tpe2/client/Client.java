package ar.edu.itba.pod.tpe2.client;

import ar.edu.itba.pod.tpe2.client.model.ReaderProvider;
import ar.edu.itba.pod.tpe2.client.query.QueryType;
import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.query.QueryStrategyProvider;
import ar.edu.itba.pod.tpe2.mapper.query1.Query1Mapper;
import ar.edu.itba.pod.tpe2.reducer.query1.Query1ReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static List<String> getAddresses(final String addresses) {
        return Arrays.stream(addresses.replace("'", "").split(";")).toList();
    }

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        logger.info("G6 Client Starting ...");

        //TODO: validation
        final String query = System.getProperty("query");
        final String addresses = System.getProperty("addresses");
        final String city = System.getProperty("city");
        final String inPath = System.getProperty("inPath");
        final String outPath = System.getProperty("outPath");

        boolean strictAgencies = false, strictInfractions = false;
        switch (query) {
            case "1":
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

        getAddresses(addresses).stream().forEach(System.out::println);

        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress("127.0.0.1");

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            // Key Value Sources
            MultiMap<String, Ticket> ticketsMultiMap = hazelcastInstance.getMultiMap("tickets");
            KeyValueSource<String, Ticket> ticketsKeyValueSource = KeyValueSource.fromMultiMap(ticketsMultiMap);

            System.out.println("ticketsMultimap.size: " + ticketsMultiMap.size());

            // CSV Files Reading and Key Value Source Loading
            // [get timestamp for file reading start]
            Date tsReadStart = new Date();
            final AtomicInteger auxKey = new AtomicInteger(); //this is to be used for the tickets "uuid"
            //idea:
            // 1. check the city selected, choose a strategy for how to read the csvs
            // 2. read through infractions and agencies csv's first to get their "full names"
            // 3. read the tickets csv, replace agency and infraction id w/ the agency name and infraction-
            //description, at no point they ask for these IDs, so maybe we can side-step having to keep
            //look-up tables for these.
            ReaderProvider.readFilesFor(city, strictAgencies, strictInfractions, ticketsMultiMap, auxKey);
            // [get timestamp for file reading end]
            Date tsReadEnd = new Date();

            System.out.println("%s INFO [main] Client - Inicio de la lectura del archivo".formatted(tsReadStart.toString()));
            System.out.println("%s INFO [main] Client - Fin de lectura del archivo".formatted(tsReadEnd.toString()));


            //Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("ticket-master");

            //Queries

            //idea:
            // [get timestamp for mapreducer start]
            // 1. pick strategy based on query number
            // 2. do query
            // 3. print output file
            // [get timestamp for mapreducers end]
            // 4. print logging file
            Date queryStart = new Date();
            Job<String, Ticket> job = jobTracker.newJob(ticketsKeyValueSource);

            QueryStrategyProvider qsp = new QueryStrategyProvider();

            QueryStrategy queryStrategy = qsp.getQueryStrategy(QueryType.selectQuery(query));
            queryStrategy.run(queryStart, job);

            /*
            ICompletableFuture<Map<InfraAgencyPair, Long>> future = job
                    .mapper(new Query1Mapper())
                    .reducer(new Query1ReducerFactory())
                    .submit();



            Map<InfraAgencyPair, Long> results  = future.get();
            results.entrySet().stream().forEach(entry -> {
                InfraAgencyPair pair = entry.getKey();
                System.out.println("%s;%s;%d".formatted(pair.getInfraction(), pair.getAgency(), entry.getValue()));
            });
            */


        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}
