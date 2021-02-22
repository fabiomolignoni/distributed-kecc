package it.unitn.msmcs.kecc;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

/**
 * Search contraction edges using an agreement approach.
 */
public class FindAgreements extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        VertexWritable state = vertex.getValue();

        if (round == 0) {
            // Generate initial neighborhood
            ArrayList<Integer> neighs = new ArrayList<Integer>();
            for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().isActive())
                    neighs.add(e.getTargetVertexId().get());
            }

            state.setContacted(contactHalfList(vertex, neighs));
        } else {
            ArrayList<IntWritable> contacted = state.getContacted();
            ArrayList<Integer> matches = new ArrayList<Integer>();

            for (ConnMessage m : messages) {
                if (m.getContent().get() == 0 && contacted.contains(m.getSender())) { // contacted match
                    // If there is a match add it to the matches list
                    matches.add(m.getSender().get());
                } else if (m.getContent().get() == 1) { // agreement proposal
                    if (!state.isActive() && !state.getMergeTarget().equals(m.getSender())) {
                        // If I already accepted another agreement, send rejection
                        sendMessage(new IntWritable(m.getSender().get()),
                                new ConnMessage(vertex.getId(), new IntWritable(-1)));
                        aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
                    } else {
                        // Accept the agreement proposal
                        state.setMergeTarget(new IntWritable(m.getSender().get()));
                        // System.out.println(vertex.getId() + " merges with " + m.getSender());
                    }
                } else if (m.getContent().get() == -1) { // rejection
                    // reset the merge target
                    state.setMergeTarget(vertex.getId());
                }
            }

            if (state.isActive()) {
                if (matches.size() == 0) {
                    // Quit if no match was found
                    state.setMergeTarget(vertex.getId());
                } else if (matches.size() == 1) {
                    // Send agreement proposal
                    IntWritable target = new IntWritable(matches.get(0));
                    sendMessage(target, new ConnMessage(vertex.getId(), new IntWritable(1)));
                    state.setMergeTarget(target);
                } else {
                    state.setContacted(contactHalfList(vertex, matches));
                }

                aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
            }
        }
    }

    /**
     * Pick half vertices from the list and send a contact message.
     * 
     * @param vertex vertex that runs the computation
     * @param list   neighborhood / contacted list from which draw randomically
     * @return list with the contacted nodes
     */
    public ArrayList<IntWritable> contactHalfList(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex,
            ArrayList<Integer> list) {
        ArrayList<IntWritable> res = new ArrayList<IntWritable>();

        Collections.shuffle(list);
        ConnMessage m = new ConnMessage(vertex.getId(), new IntWritable(0));
        for (int i = 0; i < Math.ceil(list.size() / 2.); i++) {
            IntWritable current = new IntWritable(list.get(i));
            res.add(current);
            sendMessage(current, m);
        }

        return res;
    }
}