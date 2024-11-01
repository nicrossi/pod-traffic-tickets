package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.query.query1.Query1;
import ar.edu.itba.pod.tpe2.client.query.query1.Query1A;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class QueryStrategyProvider {
    private final Map<QueryType, QueryStrategy> strategyMap;

   public QueryStrategyProvider() {
       strategyMap = new HashMap<>();
       strategyMap.put(QueryType.QUERY_1, new Query1());
       strategyMap.put(QueryType.QUERY_1A, new Query1A());
   }

    public QueryStrategy getQueryStrategy(QueryType queryType) {
        return Optional.ofNullable(strategyMap.get(queryType))
                .orElseThrow(() -> new IllegalArgumentException("Query '" + queryType + "' not supported"));
    }
}
