package ar.edu.itba.pod.tpe2.client.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum QueryType {
    QUERY_1("1"),
    QUERY_1A("1A"),
    QUERY_2("2"),
    QUERY_3("3");

    private final String queryStrNum;

    public static QueryType selectQuery(String queryNumber) throws IllegalArgumentException {
        Validate.notBlank(queryNumber, "Query type cannot be blank");
        return Arrays.stream(values())
                .filter(value -> value.queryStrNum.equalsIgnoreCase(queryNumber))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Query '" + queryNumber + "' not supported."));
    }

    @Override
    public String toString() {
        return String.format("Query %s", queryStrNum);
    }
}
