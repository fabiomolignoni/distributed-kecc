package it.unitn.msmcs.connectivity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

/**
 * If a vertex has <= k edges, then that vertex can be cut.
 */
public class PerformCut extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K_ECC)).get();
        boolean firstCut = ((BooleanWritable) getAggregatedValue(ConnectivityMaster.FIRSTCUT)).get();
        if (firstCut) {
            if (shouldCut(vertex.getEdges(), k)) {
                try {
                    for (Edge<IntWritable, ConnectivityEdgeWritable> e : vertex.getEdges()) {
                        removeEdgesRequest(new IntWritable(e.getTargetVertexId().get()), vertex.getId());
                    }
                    vertex.setEdges(new ArrayList<Edge<IntWritable, ConnectivityEdgeWritable>>());
                    removeVertexRequest(vertex.getId());
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
            }

        } else {

            if (round % 2 == 0) {
                if (vertex.getValue().getMergeTarget().get() == -1) {
                    boolean shouldCut = shouldCut(vertex.getEdges(), k);
                    if (shouldCut) {
                        // Vertex communicates the cut to all neighbors.
                        ConnectivityMessage m = new ConnectivityMessage(vertex.getId());
                        broadcastAndDeactivate(vertex.getId(), vertex.getMutableEdges(), m, k);
                    }
                }
            } else {
                HashSet<Integer> cutted = new HashSet<Integer>();
                for (ConnectivityMessage m : messages) {
                    cutted.add(m.getSender().get());
                }

                int nEdges = 0;
                for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                    if (cutted.contains(e.getTargetVertexId().get())) {
                        e.getValue().setIsActive(false);
                        e.getValue().setValue(k - 1);
                    } else if (e.getValue().isActive()) {
                        nEdges += e.getValue().getValue();
                    }
                }

                aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(nEdges > 0 && nEdges < k));

            }
        }
    }

    /**
     * 
     * @param neighbors the edges of the vertex.
     * @param k         the current connectivity value.
     * @return true if the vertex has <k edges.
     */
    private boolean shouldCut(Iterable<Edge<IntWritable, ConnectivityEdgeWritable>> edges, int k) {
        // Check if the vertex should perform a cut.
        int nEdges = 0;
        for (Edge<IntWritable, ConnectivityEdgeWritable> e : edges) {
            if (e.getValue().isActive()) {
                nEdges += e.getValue().getValue();
                if (nEdges >= k) {
                    return false;
                }
            }
        }
        return nEdges > 0;
    }

    /**
     * Send a message to all active edges and deactivate them.
     * 
     * @param edges the edges of the vertex.
     * @param m     the message to send.
     */
    private void broadcastAndDeactivate(IntWritable id,
            Iterable<MutableEdge<IntWritable, ConnectivityEdgeWritable>> edges, ConnectivityMessage m, int k) {
        // Check if the vertex should perform a cut.
        for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : edges) {
            if (e.getValue().isActive()) {
                IntWritable target = new IntWritable(e.getTargetVertexId().get());
                e.getValue().setIsActive(false);
                e.getValue().setValue(k - 1);
                sendMessage(target, new ConnectivityMessage(id));
                // System.out.println("CUT BETWEEN " + id + " " + e.getTargetVertexId());

            }
        }
    }

}