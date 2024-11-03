package ar.edu.itba.pod.tpe2.query4;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class Query4Collator implements Collator<Map.Entry<String, InfractionDiff>, List<String>> {
    private final int n;

    public Query4Collator(final int n) {
        this.n = n;
    }

    @Override
    public List<String> collate(Iterable<Map.Entry<String, InfractionDiff>> values) {
        // key = Agency:Infraction, value = InfractionDiff
        Set<InfractionDiff> resultSet = initResultMap();
        for (Map.Entry<String, InfractionDiff> entry : values) {
            resultSet.add(entry.getValue());
        }

        return resultSet.stream()
                .limit(n)
                .map(infractionDiff -> String.format("%s;%d;%d;%d",
                        infractionDiff.getInfraction(),
                        (int) infractionDiff.getMin(),
                        (int) infractionDiff.getMax(),
                        (int) infractionDiff.getDiff()))
                .toList();
    }

    private Set<InfractionDiff> initResultMap() {
        return new TreeSet<InfractionDiff>((o1, o2) -> {
            int diffCompare = Double.compare(o2.getDiff(), o1.getDiff());
            return diffCompare == 0
                    ? o1.getInfraction().compareTo(o2.getInfraction())
                    : diffCompare;
        });
    }
}