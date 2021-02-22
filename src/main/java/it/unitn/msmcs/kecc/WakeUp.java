package it.unitn.msmcs.kecc;

import java.util.ArrayList;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

public class WakeUp extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        VertexWritable state = vertex.getValue();

        if (state.getContacted().size() == 0) {
            if (state.isActive()) { // set a merge target for all vertices
                state.setMergeTarget(vertex.getId());
            }
            ArrayList<IntWritable> tmp = new ArrayList<IntWritable>();
            tmp.add(state.getMergeTarget());
            state.setContacted(tmp);
            sendMessageToAllEdges(vertex, new ConnMessage());
            aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
        }
    }
}