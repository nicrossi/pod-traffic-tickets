package ar.edu.itba.pod.tpe2.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
            //IMap<String, Ticket> ticketsIMap = hazelcastInstance.getMap("tickets");
            //KeyValueSource<String, Ticket> ticketsKeyValueSource = KeyValueSource.fromMap(ticketsIMap);

            // CSV Files Reading and Key Value Source Loading
            // [get timestamp for file reading start]
            //final AtomicInteger auxKey = new AtomicInteger(); //this is to be used for the tickets "uuid"
            //idea:
            // 1. check the city selected, choose a strategy for how to read the csvs
            // 2. read through infractions and agencies csv's first to get their "full names"
            // 3. read the tickets csv, replace agency and infraction id w/ the agency name and infraction-
            //description, at no point they ask for these IDs, so maybe we can side-step having to keep
            //look-up tables for these.
            // [get timestamp for file reading end]

            //Job Tracker
            //JobTracker jobTracker = hazelcastInstance.getJobTracker("ticket-master")

            // [get timestamp for mapreducer start]
            //Queries
            // 1. pick strategy based on query number
            // 2. do query
            // 3. print output file
            // [get timestamp for mapreducers end]
            // 4. print logging file

        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}
