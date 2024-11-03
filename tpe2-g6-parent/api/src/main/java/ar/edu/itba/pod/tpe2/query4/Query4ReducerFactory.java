package ar.edu.itba.pod.tpe2.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4ReducerFactory implements ReducerFactory<String, Double, InfractionDiff> {

    @Override
    public Reducer<Double, InfractionDiff> newReducer(String key) {
        return new Query4Reducer(key);
    }

    private static class Query4Reducer extends Reducer<Double, InfractionDiff> {
        private double min;
        private double max;
        private final String infraction;

        public Query4Reducer(String agencyInfractionKey) {
            String[] parts = agencyInfractionKey.split(":");
            this.infraction = parts[1];
        }

        @Override
        public void beginReduce() {
            min = 0D;
            max = 0D;
        }

        @Override
        public void reduce(Double fineAmount) {
            max = Math.max(max, fineAmount);
            min = Math.min(min, fineAmount);
        }

        @Override
        public InfractionDiff finalizeReduce() {
            return new InfractionDiff(min, max, max - min, infraction);
        }
    }
}
