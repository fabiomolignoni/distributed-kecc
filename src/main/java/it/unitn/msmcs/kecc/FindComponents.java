package it.unitn.msmcs.kecc;

import java.util.ArrayList;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

public class FindComponents extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        VertexWritable state = vertex.getValue();
        ArrayList<IntWritable> overlay = state.getContacted();

        if (round == 0) {
            // wakeup initializes overlay with merge target
            sendMessage(state.getMergeTarget(), new ConnMessage(vertex.getId(), state.getMergeTarget()));
        } else if (round == 1) {
            ArrayList<IntWritable> tmp = new ArrayList<IntWritable>();
            tmp.add(state.getMergeTarget());

            for (ConnMessage m : messages) {
                tmp.add(new IntWritable(m.getSender().get()));
            }
            state.setContacted(tmp);
            broadcastValue(vertex, tmp);
        } else {
            boolean updated = false;
            for (ConnMessage m : messages) {
                if (m.getContent().compareTo(state.getMergeTarget()) < 0) {
                    updated = true;
                    state.setMergeTarget(new IntWritable(m.getContent().get()));
                }
            }

            if (updated) {
                aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
                broadcastValue(vertex, overlay);
            }
        }
    }

    public void broadcastValue(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex,
            ArrayList<IntWritable> overlay) {
        for (IntWritable i : overlay) {
            sendMessage(new IntWritable(i.get()), new ConnMessage(vertex.getId(), vertex.getValue().getMergeTarget()));
        }

    }
}