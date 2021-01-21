package it.unitn.msmcs.connectivity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

public class FindComponents extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        ConnectivityStateWritable state = vertex.getValue();
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        boolean cont = false;

        if (round == 0) {
            state.setMergeTarget(vertex.getId());
            sendMessageToAllEdges(vertex, new ConnectivityMessage(vertex.getId(), new IntWritable(Integer.MAX_VALUE)));
            state.setContacted(vertex.getId());
            cont = true;

        } else {
            if (state.getContacted().length == 0) {
                if (state.getMergeTarget().get() == -1) { // Was last node of the subgraph
                    state.setMergeTarget(vertex.getId());
                } else {
                    state.setMergeTarget(state.getMergeTarget());
                }
                sendMessageToAllEdges(vertex,
                        new ConnectivityMessage(vertex.getId(), new IntWritable(Integer.MAX_VALUE)));
                state.setContacted(state.getMergeTarget());
                sendMessage(state.getMergeTarget(), new ConnectivityMessage(vertex.getId(), state.getMergeTarget()));
                cont = true;

            }

            HashSet<IntWritable> neighbors = new HashSet<IntWritable>(Arrays.asList(state.getContacted()));

            boolean updated = false;
            for (ConnectivityMessage m : messages) {
                if (m.getContent().get() < Integer.MAX_VALUE) {
                    if (!neighbors.contains(m.getSender())) {
                        IntWritable target = new IntWritable(m.getSender().get());
                        neighbors.add(target);
                        sendMessage(target, new ConnectivityMessage(vertex.getId(), state.getMergeTarget()));
                        cont = true;

                    }
                    if (state.getMergeTarget().compareTo(m.getContent()) > 0) {
                        updated = true;
                        state.setMergeTarget(new IntWritable(m.getContent().get()));
                    }
                }
            }

            if (cont == true) {
                state.setContacted(new ArrayList<IntWritable>(neighbors));
            }

            if (updated) {
                Iterator<IntWritable> itr = neighbors.iterator();
                while (itr.hasNext()) {
                    IntWritable curr = itr.next();
                    if (!curr.equals(vertex.getId())) {
                        sendMessage(curr, new ConnectivityMessage(vertex.getId(), state.getMergeTarget()));
                    }
                }
            }

            aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(cont || updated));

        }

    }
}