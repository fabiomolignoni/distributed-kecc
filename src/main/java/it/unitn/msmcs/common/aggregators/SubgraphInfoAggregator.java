package it.unitn.msmcs.common.aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import it.unitn.msmcs.common.writables.SubgraphInfoMapWritable;
import it.unitn.msmcs.common.writables.SubgraphInfoWritable;

public class SubgraphInfoAggregator extends BasicAggregator<SubgraphInfoMapWritable> {
    // TODO: remove count
    public void aggregate(SubgraphInfoMapWritable value) {
        SubgraphInfoMapWritable agg = getAggregatedValue();

        for (Writable k : value.keySet()) {
            IntWritable key = (IntWritable) k;
            if (agg.containsKey(key)) {
                SubgraphInfoWritable received = value.get(key);
                SubgraphInfoWritable stored = agg.get(key);

                if (received.getTightValue().compareTo(stored.getTightValue()) > 0
                        || (received.getTightValue().equals(stored.getTightValue())
                                && stored.getId().equals(new IntWritable(-1)))) {
                    stored.setId(new IntWritable(received.getId().get()));
                    stored.setTightValue(received.getTightValue());
                }
                agg.put(key, stored);
            } else {
                agg.put(key, value.get(key));
            }

        }
        setAggregatedValue(agg);
    }

    public SubgraphInfoMapWritable createInitialValue() {
        return new SubgraphInfoMapWritable();
    }

}
