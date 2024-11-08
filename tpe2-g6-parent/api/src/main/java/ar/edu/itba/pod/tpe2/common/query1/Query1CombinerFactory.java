package ar.edu.itba.pod.tpe2.common.query1;

import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query1CombinerFactory implements CombinerFactory<InfraAgencyPair, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(InfraAgencyPair key) {
        return new Query1Combiner();
    }

    private static class Query1Combiner extends Combiner<Long, Long> {
        private Long sum = 0L;
        @Override
        public void combine(Long value) {
            sum += value;
        }
        @Override
        public void reset() {
            sum = 0L;
        }
        @Override
        public Long finalizeChunk() {
            return sum;
        }
    }
}
