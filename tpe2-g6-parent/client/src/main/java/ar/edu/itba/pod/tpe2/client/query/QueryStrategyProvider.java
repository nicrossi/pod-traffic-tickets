package ar.edu.itba.pod.tpe2.client.query;

import ar.edu.itba.pod.tpe2.client.query.query1.Query1;


public class QueryStrategyProvider {
    public static QueryStrategy getQueryStrategy(String queryType) {
        switch (queryType) {
            case "1":
                //TODO: do the rest
            default:
                return new Query1();
        }
    }
}
