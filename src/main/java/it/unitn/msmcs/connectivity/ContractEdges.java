package it.unitn.msmcs.connectivity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

public class ContractEdges extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        ConnectivityStateWritable state = vertex.getValue();

        int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K_ECC)).get();
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();

        if (round == 0) {
            if (state.getMergeTarget().equals(vertex.getId())) {
                // Can happen after a forced contraction
                state.setMergeTarget(new IntWritable(-1));

            } else if (state.getMergeTarget().get() != -1) {
                // Communicate to new representative the neighbors.
                for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                    if (e.getValue().isActive()) {
                        // Communicate the new representative
                        IntWritable target = new IntWritable(e.getTargetVertexId().get());
                        sendMessage(target, new ConnectivityMessage(vertex.getId(), state.getMergeTarget()));
                    }
                }
            }
        } else if (round == 1) {
            HashMap<Integer, Integer> changes = new HashMap<Integer, Integer>();
            for (ConnectivityMessage m : messages) {
                changes.put(m.getSender().get(), m.getContent().get());
            }

            if (state.getMergeTarget().get() != -1) {
                for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                    if (e.getValue().isActive()) {
                        // Communicate the new representative
                        int oldTarget = e.getTargetVertexId().get();
                        int newTarget = changes.containsKey(oldTarget) ? changes.get(oldTarget) : oldTarget;
                        if (newTarget != state.getMergeTarget().get()) {
                            sendMessage(state.getMergeTarget(), new ConnectivityMessage(new IntWritable(newTarget),
                                    new IntWritable(e.getValue().getValue())));
                            if (oldTarget == newTarget) {
                                sendMessage(new IntWritable(newTarget), new ConnectivityMessage(state.getMergeTarget(),
                                        new IntWritable(e.getValue().getValue())));
                            }
                        }
                        e.getValue().setValue(Integer.MAX_VALUE);
                        e.getValue().setIsActive(false);
                    }
                }
                state.clearContacted();
                ArrayList<Edge<IntWritable, ConnectivityEdgeWritable>> edges = new ArrayList<Edge<IntWritable, ConnectivityEdgeWritable>>();
                for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                    if (e.getValue().isReal()) {
                        edges.add(EdgeFactory.create(new IntWritable(e.getTargetVertexId().get()),
                                new ConnectivityEdgeWritable(e.getValue().getValue(), true, false)));
                    }
                }
                vertex.setEdges(edges);
                vertex.voteToHalt();

            } else {
                for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                    if (changes.containsKey(e.getTargetVertexId().get())) {
                        if (e.getValue().isReal()) {
                            e.getValue().setIsActive(false);
                            e.getValue().setValue(Integer.MAX_VALUE);
                        } else {
                            try {
                                removeEdgesRequest(vertex.getId(), new IntWritable(e.getTargetVertexId().get()));
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }
                }
            }
        } else if (round == 2) {
            // Update again neighbors for new endpoints (due to link of merged nodes)
            MutablePair<Integer, Boolean> info = updateNeighbors(vertex, messages, k);
            state.clearContacted();

            aggregate(ConnectivityMaster.NEED_CUT, new BooleanWritable(info.getLeft() < k && info.getLeft() > 0));
            aggregate(ConnectivityMaster.CONSTRAINED, new BooleanWritable(info.getRight()));
            aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(info.getLeft() > 0));
        }
    }

    private MutablePair<Integer, Boolean> updateNeighbors(
            Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages, int k) {

        // Get the new neighbors
        HashMap<Integer, Integer> newNeighbors = new HashMap<Integer, Integer>();
        for (ConnectivityMessage m : messages) {
            int sender = m.getSender().get();
            int content = m.getContent().get();
            if (!newNeighbors.containsKey(sender)) {
                newNeighbors.put(sender, content);
            } else {
                newNeighbors.put(sender, newNeighbors.get(sender) + content);
            }
        }

        int nEdges = 0;
        boolean hasMerge = false;

        // Update already existing edges
        for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
            int target = e.getTargetVertexId().get();
            ConnectivityEdgeWritable eState = e.getValue();
            if (newNeighbors.containsKey(target)) {
                int size = eState.getValue() + newNeighbors.get(target);
                eState.setValue(size);
                newNeighbors.remove(target);
            }

            if (eState.isActive()) {
                nEdges += eState.getValue();
                hasMerge |= eState.getValue() >= k;
            }

        }

        // Add new edges
        for (Integer n : newNeighbors.keySet()) {
            vertex.addEdge(EdgeFactory.create(new IntWritable(n),
                    new ConnectivityEdgeWritable(newNeighbors.get(n), false, true)));
            nEdges += newNeighbors.get(n);
            hasMerge |= newNeighbors.get(n) >= k;
        }

        return new MutablePair<Integer, Boolean>(nEdges, hasMerge);
    }
}