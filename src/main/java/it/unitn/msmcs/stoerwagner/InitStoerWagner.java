package it.unitn.msmcs.stoerwagner;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;

public class InitStoerWagner
        extends BasicComputation<IntWritable, ConnectivityStateWritable, EdgeStateWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        ConnectivityStateWritable state = vertex.getValue();

        boolean isActive = state.getNumberActiveEdges().get() > 0;
        if (isActive)
            aggregate(ConnectivityMaster.REMAINING, new IntWritable(1));

        state.setMergeTarget(new IntWritable(-1));
        if (state.getSubgraph().equals(vertex.getId())) {
            state.setIsMerged(true);
            state.setIsLast(true);
            aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(isActive));

        } else {
            state.setIsMerged(false);
            state.setIsLast(false);
        }
    }
}