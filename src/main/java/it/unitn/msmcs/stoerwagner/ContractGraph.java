package it.unitn.msmcs.stoerwagner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;

public class ContractGraph
        extends BasicComputation<IntWritable, ConnectivityStateWritable, EdgeStateWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        ConnectivityStateWritable state = vertex.getValue();
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        // IntWritable k = getAggregatedValue(ConnectivityMaster.K);
        if (round == 0 && state.getMergeTarget().get() != -1) {
            // First round: merged nodes updates the neighbors of the merge targets.
            for (Edge<IntWritable, EdgeStateWritable> e : vertex.getEdges()) {
                if (e.getValue().isActive().get()) {
                    try {
                        addEdgeRequest(state.getMergeTarget(),
                                EdgeFactory.create(new IntWritable(e.getTargetVertexId().get()),
                                        new EdgeStateWritable(e.getValue(), true)));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }

            }
        } else if (round == 1 && state.getMergeTarget().get() == -1)

        {
            // Active nodes contacts their neighbors to know if they are know merged with
            // someone else.
            state.setMergeTarget(vertex.getId());
            ConnectivityMessage m = new ConnectivityMessage(vertex.getId());
            sendMessageToAllActiveEdges(vertex, m);

        } else if (round == 2) {
            // Round 3: contacted nodes reply with the ID of their representative.
            for (ConnectivityMessage m : messages) {
                sendMessage(new IntWritable(m.getSender().get()),
                        new ConnectivityMessage(vertex.getId(), state.getMergeTarget()));
            }
        } else if (round == 3) {
            if (state.getMergeTarget().equals(vertex.getId())) {
                // Non-merged nodes update their neighbors.
                ArrayList<Edge<IntWritable, EdgeStateWritable>> edges = new ArrayList<Edge<IntWritable, EdgeStateWritable>>();
                HashMap<Integer, Integer> pairing = new HashMap<Integer, Integer>();
                int nActiveEdges = 0;
                for (ConnectivityMessage m : messages) {
                    pairing.put(m.getSender().get(), m.getContent().get());
                }

                for (Edge<IntWritable, EdgeStateWritable> e : vertex.getEdges()) {
                    Integer target = pairing.get(e.getTargetVertexId().get());
                    if (e.getValue().isReal(vertex.getId())
                            && (target == null || target != e.getTargetVertexId().get())) {
                        edges.add(EdgeFactory.create(new IntWritable(e.getTargetVertexId().get()),
                                new EdgeStateWritable(e.getValue(), false)));
                    }
                    if (target != null && target != vertex.getId().get() && e.getValue().isActive().get()) {
                        edges.add(
                                EdgeFactory.create(new IntWritable(target), new EdgeStateWritable(e.getValue(), true)));
                        nActiveEdges += 1;
                    }
                }

                state.setNumberActiveEdges(nActiveEdges);
                vertex.setEdges(edges);

            } else {
                for (MutableEdge<IntWritable, EdgeStateWritable> e : vertex.getMutableEdges()) {
                    e.getValue().setIsActive(false);
                }
                state.setNumberActiveEdges(0);
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