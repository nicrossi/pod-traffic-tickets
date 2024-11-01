package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.query.query3.Query3;
import ar.edu.itba.pod.tpe2.client.query.query1.Query1;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public class QueryStrategyFactory {
    private final Map<QueryType, Function<Map<String, String>, QueryStrategy>> strategyMap = new HashMap<>();

    public QueryStrategyFactory() {
        strategyMap.put(QueryType.QUERY_1, Query1::new);
        strategyMap.put(QueryType.QUERY_3, Query3::new);
    }

    public QueryStrategy create(QueryType queryType, Map<String, String> optargs) {
        Function<Map<String, String>, QueryStrategy> constructor = strategyMap.get(queryType);
        Validate.notNull(constructor, "Query '" + queryType + "' not supported.");

        return constructor.apply(optargs);
    }
}