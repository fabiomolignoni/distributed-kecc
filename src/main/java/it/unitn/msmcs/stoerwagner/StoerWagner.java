package it.unitn.msmcs.connectivity;

import java.util.HashMap;
import java.util.HashSet;

import com.google.common.collect.Iterators;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;
import it.unitn.msmcs.common.writables.SubgraphInfoMapWritable;
import it.unitn.msmcs.common.writables.SubgraphInfoWritable;

public class StoerWagner
        extends BasicComputation<IntWritable, ConnectivityStateWritable, EdgeStateWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        ConnectivityStateWritable state = vertex.getValue();
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();

        if (round == 0) {
            if (state.isMerged().get()) {
                // First round: merged nodes send a message to all its neighbors
                contactUnmergedNodes(vertex);
            }

            // Check if some vertex communicate to merge/cut
            if (Iterators.size(messages.iterator()) > 0) {
                performMergeOrCut(vertex, messages);
            }

        } else if (round == 1 && !state.isMerged().get()) {
            // Second round: unmerged nodes check who is the tightest.
            checkTightness(vertex, messages);

        } else if (round == 2) {
            computeNewLast(vertex);

        }

    }

    /**
     * First round, send a message to everyone.
     * 
     * @param vertex
     */
    public void contactUnmergedNodes(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex) {
        ConnectivityMessage m = new ConnectivityMessage(vertex.getId(), vertex.getValue().getSubgraph());
        sendMessageToAllActiveEdges(vertex, m);
    }

    public void performMergeOrCut(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        ConnectivityStateWritable state = vertex.getValue();
        IntWritable k = getAggregatedValue(ConnectivityMaster.K);

        for (ConnectivityMessage m : messages) {
            if (m.getContent().compareTo(k) > 0) {
                state.setMergeTarget(m.getSender());
                // System.out.println(vertex.getId() + " will merge with " + m.getSender());
            } else {
                for (MutableEdge<IntWritable, EdgeStateWritable> e : vertex.getMutableEdges()) {
                    if (e.getTargetVertexId().equals(m.getSender())) {
                        if (e.getValue().isActive().get()) {
                            state.decreaseNumberActiveEdges();
                        }
                        // System.out.println(vertex.getId() + " will cut with " +
                        // e.getTargetVertexId());
                        e.getValue().setIsActive(false);
                        e.getValue().setValue(k);
                    }
                }
            }
        }
    }

    /**
     * 
     * @param vertex
     * @param messages
     * @param state
     */
    public void checkTightness(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        ConnectivityStateWritable state = vertex.getValue();
        HashMap<Integer, Integer> tightness = new HashMap<Integer, Integer>();
        for (ConnectivityMessage m : messages) {
            // count messages received
            if (!tightness.containsKey(m.getContent().get())) {
                tightness.put(m.getContent().get(), 0);
            }

            tightness.put(m.getContent().get(), tightness.get(m.getContent().get()) + 1);
        }

        // propose as tightest
        int myId = state.isMerged().get() ? -1 : vertex.getId().get();
        SubgraphInfoMapWritable m = new SubgraphInfoMapWritable();
        for (int k : tightness.keySet()) {
            if (k != state.getSubgraph().get()) {
                m.put(new IntWritable(k), new SubgraphInfoWritable(new IntWritable(myId), tightness.get(k)));
            }
        }
        aggregate(ConnectivityMaster.SUBGRAPH_INFO, m);

    }

    private void computeNewLast(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex) {
        // Third round: merge the node and prepare for contract if only one node is left
        SubgraphInfoMapWritable mapTightly = getAggregatedValue(ConnectivityMaster.SUBGRAPH_INFO);
        ConnectivityStateWritable state = vertex.getValue();
        SubgraphInfoWritable subgraphInfo = (SubgraphInfoWritable) mapTightly.get(state.getSubgraph());
        IntWritable k = getAggregatedValue(ConnectivityMaster.K);

        if (state.isLast().get() && subgraphInfo != null && subgraphInfo.getId().get() != -1
                && subgraphInfo.getTightValue().compareTo(k) > 0) {
            // Communicate to last node with whom it has to merge
            if (state.getMergeTarget().get() == -1) {
                sendMessage(subgraphInfo.getId(),
                        new ConnectivityMessage(vertex.getId(), subgraphInfo.getTightValue()));
            } else {
                sendMessage(subgraphInfo.getId(),
                        new ConnectivityMessage(state.getMergeTarget(), subgraphInfo.getTightValue()));
            }

        } else if (state.getNumberActiveEdges().compareTo(k) <= 0) {
            // Communicate to vertices to perform the cut
            for (MutableEdge<IntWritable, EdgeStateWritable> e : vertex.getMutableEdges()) {
                if (e.getValue().isActive().get()) {
                    IntWritable from = new IntWritable(e.getValue().getFrom().get());
                    IntWritable to = new IntWritable(e.getValue().getTo().get());
                    sendMessage(from, new ConnectivityMessage(to, k));
                    sendMessage(to, new ConnectivityMessage(from, k));

                    e.getValue().setValue(k);
                    e.getValue().setIsActive(false); // perform cut
                    state.decreaseNumberActiveEdges();
                    sendMessage(new IntWritable(e.getTargetVertexId().get()),
                            new ConnectivityMessage(vertex.getId(), k));
                }
            }
        }

        // Update the status of the vertex.
        if (state.isMerged().get()) {
            state.setIsLast(false);

        } else {
            for (IntWritable key : mapTightly.keySet()) {
                if (mapTightly.get(key).getId().equals(vertex.getId())) {
                    // If I am the chosen one I merge
                    state.setIsMerged(true);
                    state.setIsLast(true);
                    state.setSubgraph(new IntWritable(key.get()));
                    aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
                    break;
                }
            }
        }
    }

    private void sendMessageToAllActiveEdges(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            ConnectivityMessage message) {
        for (Edge<IntWritable, EdgeStateWritable> e : vertex.getEdges()) {
            if (e.getValue().isActive().get()) {
                sendMessage(new IntWritable(e.getTargetVertexId().get()), message);
            }
        }
    }
}