package it.unitn.msmcs.kecc;

import java.util.HashMap;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

public class ContractGraph extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    /**
     * Contract edges that found an agreement together. Vertex with smallest id
     * remains active.
     */
    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        VertexWritable state = vertex.getValue();
        if (round == 0) {
            vertex.getValue().clearContacted();
            // Keep actives smallest id vertices and the one who didn't found a cut
            if (!state.isActive() && (vertex.getId().compareTo(state.getMergeTarget()) <= 0)) {
                state.setMergeTarget(new IntWritable(-1));
            }

            if (!state.isActive()) {
                // Communicate to neighbors the new representative
                ConnMessage m = new ConnMessage(vertex.getId(), state.getMergeTarget());
                for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                    if (e.getValue().isActive()) {
                        sendMessage(new IntWritable(e.getTargetVertexId().get()), m);
                    }
                }
            }
        } else if (round == 1) {
            // Get new representatives of neighbors
            HashMap<Integer, Integer> reprs = new HashMap<Integer, Integer>();
            for (ConnMessage m : messages) {
                reprs.put(m.getSender().get(), m.getContent().get());
            }

            if (state.isActive()) {
                // if vertex is active deactivates old neighbors
                for (MutableEdge<IntWritable, EdgeWritable> e : vertex.getMutableEdges()) {
                    if (reprs.keySet().contains(e.getTargetVertexId().get())) {
                        e.getValue().setIsActive(false);
                    }
                }
            } else {
                // send new references
                for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                    EdgeWritable eState = e.getValue();
                    if (eState.isActive()) {
                        eState.setIsActive(false);
                        Integer target = e.getTargetVertexId().get();
                        if (reprs.keySet().contains(target)) {
                            target = reprs.get(target);
                        }
                        if (target != state.getMergeTarget().get()) {
                            // communicate neighbor to mergeTarget
                            sendMessage(state.getMergeTarget(),
                                    new ConnMessage(new IntWritable(target), new IntWritable(eState.getSize())));

                            if (target == e.getTargetVertexId().get()) {
                                // if neighbor did not merge communicate new reference
                                sendMessage(new IntWritable(target),
                                        new ConnMessage(state.getMergeTarget(), new IntWritable(eState.getSize())));
                            }
                        }
                    }
                }
                vertex.voteToHalt();
            }
        } else if (round == 2 && state.isActive()) {
            HashMap<Integer, Integer> newNeighs = new HashMap<Integer, Integer>();
            int nEdges = 0;
            boolean forced = false;
            int k = ((IntWritable) getAggregatedValue(ConnectivityMaster.K)).get();
            // get new edges
            for (ConnMessage m : messages) {
                if (!newNeighs.keySet().contains(m.getSender().get())) {
                    newNeighs.put(m.getSender().get(), m.getContent().get());
                } else {
                    newNeighs.put(m.getSender().get(), newNeighs.get(m.getSender().get()) + m.getContent().get());
                }
            }

            // update already existing edges
            for (MutableEdge<IntWritable, EdgeWritable> e : vertex.getMutableEdges()) {
                int target = e.getTargetVertexId().get();
                if (newNeighs.keySet().contains(target)) {
                    int currSize = e.getValue().getSize();
                    int total = currSize + newNeighs.get(target);
                    forced |= total >= k;
                    e.getValue().setSize(total);
                    nEdges += total;
                    newNeighs.remove(target);
                } else if (e.getValue().isActive()) {
                    nEdges += e.getValue().getSize();
                    forced |= e.getValue().getSize() >= k;
                }
            }

            // create new edges
            for (Integer n : newNeighs.keySet()) {
                vertex.addEdge(EdgeFactory.create(new IntWritable(n), new EdgeWritable(newNeighs.get(n), false, true)));
                nEdges += newNeighs.get(n);
                forced |= newNeighs.get(n) >= k;
            }

            // decide next operation
            aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(nEdges > 0));
            aggregate(ConnectivityMaster.CUT, new BooleanWritable(nEdges > 0 && nEdges < k));
            aggregate(ConnectivityMaster.FORCED, new BooleanWritable(forced));
        }
    }
}