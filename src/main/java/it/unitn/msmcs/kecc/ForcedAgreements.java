package it.unitn.msmcs.kecc;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

public class ForcedAgreements extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K)).get();
        VertexWritable state = vertex.getValue();

        if (round == 0) {
            boolean found = true;
            state.setMergeTarget(vertex.getId());
            ConnMessage m = new ConnMessage(vertex.getId(), vertex.getId());

            for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().getSize() >= k && e.getValue().isActive()) {
                    // If I am part of a k-component with n, I send my id to that neighbor
                    sendMessage(new IntWritable(e.getTargetVertexId().get()), m);
                    found = true;
                }
            }
            // Continue only if I am part of a k-component
            aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(found));
        } else {
            boolean updated = false;

            for (ConnMessage m : messages) {
                if (m.getContent().compareTo(state.getMergeTarget()) < 0) {
                    // update merge target only if the received id is smaller than the current value
                    state.setMergeTarget(new IntWritable(m.getContent().get()));
                    updated = true;
                }
            }

            if (updated) {
                // If I updated the value, I broadcast the new knowledge
                ConnMessage m = new ConnMessage(vertex.getId(), state.getMergeTarget());
                for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                    if (e.getValue().getSize() >= k && e.getValue().isActive()) {
                        sendMessage(new IntWritable(e.getTargetVertexId().get()), m);
                    }
                }
                aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
            }
        }

    }
}