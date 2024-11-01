package ar.edu.itba.pod.tpe2.common.query1;

import ar.edu.itba.pod.tpe2.common.InfraAgencyPair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query1ReducerFactory implements ReducerFactory<InfraAgencyPair, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(InfraAgencyPair key) {
        return new Query1Reducer();
    }

    private static class Query1Reducer extends Reducer<Long, Long> {
        private Long sum = 0L;

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce(){
            return sum;
        }
    }
}
