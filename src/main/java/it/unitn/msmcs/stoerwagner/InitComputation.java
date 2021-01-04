package it.unitn.msmcs.stoerwagner;

import java.util.HashSet;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
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

        if (getSuperstep() == 0) {
            // Communicate links to neighbors
            for (MutableEdge<IntWritable, EdgeStateWritable> e : vertex.getMutableEdges()) {
                if (!vertex.getId().equals(e.getTargetVertexId())) {
                    sendMessage(new IntWritable(e.getTargetVertexId().get()), new ConnectivityMessage(vertex.getId()));
                } else {
                    e.getValue().setIsActive(false);
                }
            }

        } else {
            // Get new neighbors
            HashSet<Integer> newNeighs = new HashSet<Integer>();
            for (ConnectivityMessage m : messages) {
                newNeighs.add(m.getSender().get());
            }

            // Remove already-existing neighbors
            for (MutableEdge<IntWritable, EdgeStateWritable> e : vertex.getMutableEdges()) {
                newNeighs.remove(e.getTargetVertexId().get());
            }

            // Add new neighbors
            for (int n : newNeighs) {
                IntWritable target = new IntWritable(n);
                vertex.getValue().increaseNumberActiveEdges();
                vertex.addEdge(EdgeFactory.create(target,
                        new EdgeStateWritable(vertex.getId(), target, new IntWritable(0), new BooleanWritable(true))));
            }
            vertex.getValue().setNumberActiveEdges(vertex.getNumEdges());
        }
    }

}