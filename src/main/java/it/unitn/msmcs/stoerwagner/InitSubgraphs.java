package it.unitn.msmcs.connectivity;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;

public class InitSubgraphs
        extends BasicComputation<IntWritable, ConnectivityStateWritable, EdgeStateWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        ConnectivityStateWritable state = vertex.getValue();

        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        if (round == 0) {
            aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
            state.setSubgraph(vertex.getId());
            ConnectivityMessage message = new ConnectivityMessage(vertex.getId(), vertex.getId());
            sendMessageToAllActiveEdges(vertex, message);

        } else {
            boolean changed = false;
            for (ConnectivityMessage m : messages) {
                changed = changed || state.updateSubgraph(m.getContent());
            }

            if (changed) {
                aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
                ConnectivityMessage message = new ConnectivityMessage(vertex.getId(), state.getSubgraph());
                sendMessageToAllActiveEdges(vertex, message);
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