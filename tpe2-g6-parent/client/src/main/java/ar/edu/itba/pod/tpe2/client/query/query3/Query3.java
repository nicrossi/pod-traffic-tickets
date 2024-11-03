package ar.edu.itba.pod.tpe2.client.query.query3;

import ar.edu.itba.pod.tpe2.client.query.QueryStrategy;
import ar.edu.itba.pod.tpe2.client.utils.DateUtils;
import ar.edu.itba.pod.tpe2.client.utils.Writer;
import ar.edu.itba.pod.tpe2.query3.Query3Collator;
import ar.edu.itba.pod.tpe2.common.Ticket;
import ar.edu.itba.pod.tpe2.query3.Query3Mapper;
import ar.edu.itba.pod.tpe2.query3.Query3ReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@NoArgsConstructor(force = true)
public class Query3 implements QueryStrategy {
    private static final String[] headers = {"County", "Percentage"};
    private final Map<String, String> args;

    public Query3(Map<String, String> args) {
        Validate.notNull(args, "Query 3, parameter 'args' cannot be null");
        Validate.isTrue(args.containsKey("from"), "Query 3, parameter 'from' is required");
        Validate.isTrue(args.containsKey("to"), "Query 3, parameter 'to' is required");
        Validate.isTrue(args.containsKey("n"), "Query 3, parameter 'n' is required");
        this.args = args;
    }

    @Override
    public void run(Writer writer , Job<String, Ticket> job) throws ExecutionException, InterruptedException {
        Date mpStart = new Date();
        writer.addLog("%s INFO [main] Client - Inicio del trabajo map/reduce", mpStart);

        LocalDateTime from = DateUtils.parseDate(args.get("from"));
        LocalDateTime to = DateUtils.parseDate(args.get("to")).withHour(23).withMinute(59).withSecond(59);
        int n = Integer.parseInt(args.get("n"));
        ICompletableFuture<List<String>> future = job
                .mapper(new Query3Mapper(from, to))
                .reducer(new Query3ReducerFactory(n))
                .submit(new Query3Collator());

        List<String> results = future.get();
        results.forEach(writer::addResult);

        writer.outputResults(headers);

        Date mpEnd = new Date();
        writer.addLog("%s INFO [main] Client - Fin del trabajo map/reduce", mpEnd);

    }
}
