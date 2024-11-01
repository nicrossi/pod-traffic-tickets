package ar.edu.itba.pod.tpe2.query2;

import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Query2Collator implements Collator<Map.Entry<String, Double>, List<String>> {
    @Override
    public List<String> collate(Iterable<Map.Entry<String, Double>> values) {
        Map<String, Map<String, Double[]>> agencyRevenueMap = new TreeMap<>();

        for (Map.Entry<String, Double> entry : values) {
            String[] parts = entry.getKey().split(":");
            String agency = parts[0];
            String year = parts[1];
            String month = parts[2];
            double amount = entry.getValue();

            agencyRevenueMap
                    .computeIfAbsent(agency, k -> new TreeMap<>())
                    .computeIfAbsent(year, k -> new Double[12])[Integer.parseInt(month) - 1] = amount;
        }

        return computeYTD(agencyRevenueMap);
    }

    private List<String> computeYTD(Map<String, Map<String, Double[]>> agencyRevenueMap) {
        List<String> results = new ArrayList<>();
        for (Map.Entry<String, Map<String, Double[]>> agencyEntry : agencyRevenueMap.entrySet()) {
            String agency = agencyEntry.getKey();
            // key = YYYY, value = [jan-revenue, feb-revenue, ..., dec-revenue]
            Map<String, Double[]> yearMonthMap = agencyEntry.getValue();
            for (Map.Entry<String, Double[]> yearEntry : yearMonthMap.entrySet()) {
                String year = yearEntry.getKey();
                Double[] monthArray = yearEntry.getValue();
                double ytd = 0;
                for (int i = 0; i < monthArray.length; i++) {
                    if (monthArray[i] == null) {
                        continue;
                    }
                    ytd += monthArray[i];
                    results.add(agency + ";" + year + ";" + (i + 1) + ";" + (int)ytd);
                }
            }
        }
        return results;
    }
}
