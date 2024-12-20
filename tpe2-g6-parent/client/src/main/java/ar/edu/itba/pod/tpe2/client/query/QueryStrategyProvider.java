package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.query.query1.Query1A;
import ar.edu.itba.pod.tpe2.client.query.query2.Query2;
import ar.edu.itba.pod.tpe2.client.query.query2.Query2A;
import ar.edu.itba.pod.tpe2.client.query.query3.Query3;
import ar.edu.itba.pod.tpe2.client.query.query1.Query1;
import ar.edu.itba.pod.tpe2.client.query.query3.Query3A;
import ar.edu.itba.pod.tpe2.client.query.query4.Query4;
import ar.edu.itba.pod.tpe2.client.query.query4.Query4A;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class QueryStrategyProvider {
    private final Map<QueryType, QueryStrategy> strategyMap;
    private final QueryStrategyFactory queryStrategyFactory;

   public QueryStrategyProvider() {
       queryStrategyFactory = new QueryStrategyFactory();
       strategyMap = new HashMap<>();
       strategyMap.put(QueryType.QUERY_1, new Query1());
       strategyMap.put(QueryType.QUERY_2, new Query2());
       strategyMap.put(QueryType.QUERY_3, new Query3());
       strategyMap.put(QueryType.QUERY_4, new Query4());
       // no combiner alternatives:
       strategyMap.put(QueryType.QUERY_1A, new Query1A());
       strategyMap.put(QueryType.QUERY_2A, new Query2A());
       strategyMap.put(QueryType.QUERY_3A, new Query3A());
       strategyMap.put(QueryType.QUERY_4A, new Query4A());
   }

    public QueryStrategy getQueryStrategy(QueryType queryType, @NonNull Map<String, String> optargs) {
        QueryStrategy qs = Optional.ofNullable(strategyMap.get(queryType))
                .orElseThrow(() -> new IllegalArgumentException("Query '" + queryType + "' not supported"));

        return optargs.isEmpty() ? qs : queryStrategyFactory.create(queryType, optargs);
    }
}
