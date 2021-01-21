package it.unitn.msmcs.connectivity;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

public class RestoreGraph extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K_ECC)).get();
        ConnectivityStateWritable state = vertex.getValue();

        if (round == 0) {
            for (Edge<IntWritable, ConnectivityEdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().isReal() && e.getValue().getValue() >= k - 1) {
                    sendMessage(e.getTargetVertexId(), new ConnectivityMessage(vertex.getId(), state.getMergeTarget()));
                }
            }
        } else {
            ArrayList<Integer> cutted = new ArrayList<Integer>();

            for (ConnectivityMessage m : messages) {
                if (!m.getContent().equals(state.getMergeTarget())) {
                    cutted.add(m.getSender().get());
                }
            }

            ArrayList<Edge<IntWritable, ConnectivityEdgeWritable>> edges = new ArrayList<Edge<IntWritable, ConnectivityEdgeWritable>>();
            boolean hasActive = false;
            for (Edge<IntWritable, ConnectivityEdgeWritable> e : vertex.getEdges()) {
                ConnectivityEdgeWritable edgeState = e.getValue();
                if (edgeState.isReal() && !cutted.contains(e.getTargetVertexId().get())) {
                    hasActive = true;
                    edges.add(EdgeFactory.create(new IntWritable(e.getTargetVertexId().get()),
                            new ConnectivityEdgeWritable(1, true, true)));
                }
            }

            state.clearContacted();
            state.setMergeTarget(new IntWritable(-1));
            vertex.setEdges(edges);

            if (!hasActive) {
                try {
                    removeVertexRequest(vertex.getId());
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

    }
}