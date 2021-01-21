package it.unitn.msmcs.connectivity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

/**
 * Search contraction edges using an agreement approach.
 */
public class FindAgreement extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        ConnectivityStateWritable state = vertex.getValue();
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();

        if (round == 0 && state.isActive()) {

            List<IntWritable> neighbors = getActiveNeighbors(vertex.getEdges());
            if (neighbors.size() > 0) {
                ArrayList<IntWritable> contacted = sendToHalfNeighbors(vertex, neighbors);
                state.setContacted(contacted);
            } else {
                state.setMergeTarget(vertex.getId());
            }

        } else {
            if (!state.isActive()) {
                for (ConnectivityMessage m : messages) {
                    if (m.getSender().equals(state.getMergeTarget()) && m.getContent().get() == -2) {
                        state.setMergeTarget(vertex.getId());
                    } else if (m.getContent().get() == -1 && !m.getSender().equals(state.getContacted()[0])) {
                        sendMessage(new IntWritable(m.getSender().get()),
                                new ConnectivityMessage(vertex.getId(), new IntWritable(-2)));
                        aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
                    }
                }
            } else {
                // Pick matches
                HashSet<IntWritable> contacted = new HashSet<IntWritable>(Arrays.asList(state.getContacted()));
                ArrayList<IntWritable> matches = new ArrayList<IntWritable>();

                // Pick matches
                boolean found = false;
                for (ConnectivityMessage m : messages) {
                    if (contacted.contains(m.getSender()) && m.getContent().get() >= 0) {
                        // Matches between two nodes
                        matches.add(new IntWritable(m.getSender().get()));

                    } else if (m.getContent().get() == -1 && !found) {
                        // First merge request
                        found = true;
                        IntWritable current = new IntWritable(m.getSender().get());
                        IntWritable target = vertex.getId().compareTo(current) > 0 ? current : vertex.getId();
                        state.setMergeTarget(target);
                        state.setContacted(current);
                        // System.out.println("MERGE BETWEEN " + vertex.getId() + " - " + current);

                    } else if (m.getContent().get() == -1 && found) {
                        // Reject other merge requests
                        sendMessage(new IntWritable(m.getSender().get()),
                                new ConnectivityMessage(vertex.getId(), new IntWritable(-2)));

                    }
                }

                if (state.isActive()) { // i.e. did not receive any merge message

                    if (matches.size() == 0) {
                        state.setMergeTarget(vertex.getId());

                    } else if (matches.size() == 1) {
                        // System.out.println(vertex.getId() + " proposes to " + matches.get(0) + "
                        // round " + round);
                        sendMessage(matches.get(0), new ConnectivityMessage(vertex.getId(), new IntWritable(-1)));
                        IntWritable target = vertex.getId().compareTo(matches.get(0)) > 0 ? matches.get(0)
                                : vertex.getId();
                        state.setMergeTarget(target);
                        state.setContacted(matches.get(0));

                    } else {
                        ArrayList<IntWritable> newContacted = sendToHalfNeighbors(vertex, matches);
                        state.setContacted(newContacted);
                    }
                }
                aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
            }
        }
    }

    /**
     * @param edges edges of the vertex.
     * @return Returns only the active edges.
     */
    private ArrayList<IntWritable> getActiveNeighbors(Iterable<Edge<IntWritable, ConnectivityEdgeWritable>> edges) {
        ArrayList<IntWritable> res = new ArrayList<IntWritable>();

        for (Edge<IntWritable, ConnectivityEdgeWritable> e : edges) {
            if (e.getValue().isActive()) {
                res.add(new IntWritable(e.getTargetVertexId().get()));
            }
        }
        return res;
    }

    ArrayList<IntWritable> sendToHalfNeighbors(
            Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            List<IntWritable> neighbors) {
        Collections.shuffle(neighbors);
        long calls = Math.max(1, Math.round(neighbors.size() * 0.5));
        ArrayList<IntWritable> contacted = new ArrayList<IntWritable>();
        for (int i = 0; i < calls; i++) {
            IntWritable current = new IntWritable(neighbors.get(i).get());
            contacted.add(current);
            sendMessage(current, new ConnectivityMessage(vertex.getId(), vertex.getId()));
        }

        return contacted;
    }
}