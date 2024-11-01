package ar.edu.itba.pod.tpe2.query3;

import com.hazelcast.mapreduce.Collator;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query3Collator implements Collator<Map.Entry<String, Map<String, Long>>, List<String>> {

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Map<String, Long>>> values) {
        // Aggregate the data by county
        Map<String, Map<String, Long>> countyAggregates = aggregateDataByCounty(values);

        // Calculate the percentage of repeat offenders for each county
        Map<String, Double> countyPercentages = calculatePercentages(countyAggregates);

        // Sort and format the results, then add the header
        return formatResults(countyPercentages);
    }

    private Map<String, Map<String, Long>> aggregateDataByCounty(Iterable<Map.Entry<String, Map<String, Long>>> values) {
        Map<String, Map<String, Long>> countyAggregates = new HashMap<>();

        values.forEach(entry -> {
            String county = entry.getKey().split(":")[0];
            Map<String, Long> stats = entry.getValue();

            countyAggregates.computeIfAbsent(county, k -> new HashMap<>())
                    .merge("repeatOffenderCount", stats.get("repeatOffenderCount"), Long::sum);
            countyAggregates.get(county)
                    .merge("uniquePlateCount", stats.get("uniquePlateCount"), Long::sum);
        });

        return countyAggregates;
    }

    private Map<String, Double> calculatePercentages(Map<String, Map<String, Long>> countyAggregates) {
        return countyAggregates.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> (double) entry.getValue()
                                .get("repeatOffenderCount") / entry.getValue()
                                .get("uniquePlateCount") * 100
                ));
    }

    private List<String> formatResults(Map<String, Double> countyPercentages) {
        return Stream.concat(
                Stream.of("County;Percentage"),
                // Sort by percentage in descending order, then by county name alphabetically
                countyPercentages.entrySet().stream()
                        .sorted((e1, e2) -> {
                            int cmp = e2.getValue().compareTo(e1.getValue());
                            if (cmp == 0) {
                                return e1.getKey().compareTo(e2.getKey());
                            }
                            return cmp;
                        })
                        .map(entry -> String.format("%s;%.2f%%",
                                entry.getKey(),
                                truncate2Decimals(entry.getValue())))
        ).collect(Collectors.toList());
    }

    private double truncate2Decimals(double number) {
        Validate.isTrue(number >= 0, "Number must be positive");
        return Math.floor(number * 100) / 100;
    }
}