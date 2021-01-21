package it.unitn.msmcs.connectivity;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

public class ConstrainedContract extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        ConnectivityStateWritable state = vertex.getValue();
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K_ECC)).get();

        if (round == 0) {
            int minId = vertex.getId().get();
            boolean found = false;
            for (Edge<IntWritable, ConnectivityEdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().isActive() && e.getValue().getValue() >= k) {
                    minId = Math.min(minId, e.getTargetVertexId().get());
                    found = true;
                }
            }
            if (found) {
                state.setMergeTarget(new IntWritable(minId));
                broadcastOnKConnectedEdges(vertex, minId, k);
            }
        } else {
            boolean updated = false;
            for (ConnectivityMessage m : messages) {
                if (m.getContent().compareTo(state.getMergeTarget()) < 0 && !m.getContent().equals(vertex.getId())) {
                    state.setMergeTarget(new IntWritable(m.getContent().get()));
                    updated = true;
                }
            }

            if (updated) {
                broadcastOnKConnectedEdges(vertex, state.getMergeTarget().get(), k);
                aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
            }

        }

    }

    private void broadcastOnKConnectedEdges(
            Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex, int minId, int k) {
        ConnectivityMessage m = new ConnectivityMessage(vertex.getId(), new IntWritable(minId));
        for (Edge<IntWritable, ConnectivityEdgeWritable> e : vertex.getEdges()) {
            if (e.getValue().isActive() && e.getValue().getValue() >= k) {
                sendMessage(e.getTargetVertexId(), m);
            }
        }
    }
}