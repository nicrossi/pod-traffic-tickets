package ar.edu.itba.pod.tpe2.query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2ReducerFactory implements ReducerFactory<String, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(String key) {
        return new Query2Reducer();
    }

    private static class Query2Reducer extends Reducer<Double, Double> {
        private double sum;

        @Override
        public void beginReduce() {
            sum = 0D;
        }

        @Override
        public void reduce(Double value) {
            sum += value;
        }

        @Override
        public Double finalizeReduce() {
            return sum;
        }
    }
}
