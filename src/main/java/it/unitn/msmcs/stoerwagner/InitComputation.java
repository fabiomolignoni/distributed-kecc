package it.unitn.msmcs.connectivity;

import java.io.IOException;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;

public class InitComputation
        extends BasicComputation<IntWritable, ConnectivityStateWritable, EdgeStateWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        boolean toDirected = !getConf().get("input.directed").equals("true");
        if (getSuperstep() == 0) {
            vertex.getValue().setNumberActiveEdges(vertex.getNumEdges());
            if (toDirected) {
                sendMessageToAllEdges(vertex, new ConnectivityMessage(vertex.getId()));
            }
        } else if (toDirected) {
            try {
                for (ConnectivityMessage m : messages) {
                    IntWritable target = new IntWritable(m.getSender().get());
                    vertex.getValue().increaseNumberActiveEdges();
                    addEdgeRequest(vertex.getId(), EdgeFactory.create(target, new EdgeStateWritable(vertex.getId(),
                            target, new IntWritable(0), new BooleanWritable(true))));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}