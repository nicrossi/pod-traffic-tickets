package ar.edu.itba.pod.tpe2.query2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query2CombinerFactory implements CombinerFactory<String, Double, Double> {
    @Override
    public Combiner<Double, Double> newCombiner(String key) {
        return new Query2Combiner();
    }

    private static class Query2Combiner extends Combiner<Double, Double> {
        private double sum = 0L;
        @Override
        public void combine(Double value) {
            sum += value;
        }
        @Override
        public void reset() {
            sum = 0D;
        }
        @Override
        public Double finalizeChunk() {
            return sum;
        }
    }
}
