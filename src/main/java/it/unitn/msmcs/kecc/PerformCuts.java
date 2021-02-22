package it.unitn.msmcs.kecc;

import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

/**
 * If a vertex has less than k edges we perform a cut.
 */
public class PerformCuts extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K)).get();
        if (round % 2 == 0) {
            if (shouldCut(vertex, k)) {
                // set yourself as merge target
                vertex.getValue().setMergeTarget(vertex.getId());

                // Communicate the cut to all active neighbors
                for (MutableEdge<IntWritable, EdgeWritable> e : vertex.getMutableEdges()) {
                    if (e.getValue().isActive()) {
                        e.getValue().setIsActive(false);
                        sendMessage(new IntWritable(e.getTargetVertexId().get()),
                                new ConnMessage(vertex.getId(), new IntWritable(-1)));
                    }
                }
            } else {
                aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
            }

        } else {
            // Read cut neighbors
            HashSet<Integer> cuts = new HashSet<Integer>();
            for (ConnMessage m : messages) {
                cuts.add(m.getSender().get());
            }

            // Update active neighbors
            int nEdges = 0;
            for (MutableEdge<IntWritable, EdgeWritable> e : vertex.getMutableEdges()) {
                EdgeWritable eState = e.getValue();
                if (cuts.contains(e.getTargetVertexId().get())) {
                    eState.setIsActive(false);
                    eState.setSize(-1);
                } else if (eState.isActive()) {
                    nEdges += eState.getSize();
                }
            }

            // Halt cut vertices
            if (!vertex.getValue().isActive()) {
                BooleanWritable active = getAggregatedValue(ConnectivityMaster.CONTINUE);
                if (active.get())
                    vertex.voteToHalt();
            } else {
                // Decide whether to continue with the cut
                aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(nEdges < k));
            }
        }
    }

    private boolean shouldCut(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, int k) {
        int edges = 0;
        for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
            EdgeWritable eState = e.getValue();
            if (eState.isActive()) {
                edges += eState.getSize();
            }
            if (edges >= k) {
                return false;
            }
        }

        return true;
    }
}