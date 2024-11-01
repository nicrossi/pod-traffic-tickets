package ar.edu.itba.pod.tpe2.common.query1;

import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Query1Collator
        implements Collator<Map.Entry<InfraAgencyPair, Long>, SortedSet<Query1Result>> {
    @Override
    public SortedSet<Query1Result> collate(Iterable<Map.Entry<InfraAgencyPair, Long>> entries) {
        SortedSet<Query1Result> results = new TreeSet<>(
                Comparator.comparing(Query1Result::tickets).reversed()
                        .thenComparing(Query1Result::agency)
        );

        for (Map.Entry<InfraAgencyPair, Long> entry : entries) {
            results.add(new Query1Result(
                    entry.getKey().getInfraction(),
                    entry.getKey().getAgency(),
                    entry.getValue()));
        }

        return results;
    }
}
