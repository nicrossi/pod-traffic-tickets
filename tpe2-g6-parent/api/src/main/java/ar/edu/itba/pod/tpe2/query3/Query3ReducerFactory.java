package ar.edu.itba.pod.tpe2.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Query3ReducerFactory implements ReducerFactory<String, String, Map<String, Long>> {
    private final int n;

    public Query3ReducerFactory(int n) {
        this.n = n;
    }

    @Override
    public Reducer<String, Map<String, Long>> newReducer(String key) {
        return new Query3Reducer(key, n);
    }

    private static class Query3Reducer extends Reducer<String, Map<String, Long>> {
        private final String key;
        private final int n;
        private Map<String, Long> plateCountMap;
        private Set<String> uniquePlates;

        public Query3Reducer(String key, int n) {
            this.key = key;
            this.n = n;
        }

        @Override
        public void beginReduce() {
            plateCountMap = new HashMap<>();
            uniquePlates = new HashSet<>();
        }

        @Override
        public void reduce(String plate) {
            plateCountMap.put(plate, plateCountMap.getOrDefault(plate, 0L) + 1);
            uniquePlates.add(plate);
        }

        @Override
        public Map<String, Long> finalizeReduce() {
            Map<String, Long> result = new HashMap<>();
            long repeatOffenderCount = plateCountMap.values().stream().filter(count -> count >= n).count();
            result.put("repeatOffenderCount", repeatOffenderCount);
            result.put("uniquePlateCount", (long) uniquePlates.size());
            return result;
        }
    }
}
