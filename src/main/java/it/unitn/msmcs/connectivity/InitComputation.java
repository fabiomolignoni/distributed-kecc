package it.unitn.msmcs.connectivity;

import java.util.HashSet;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;

/**
 * If the input graph is undirected it adds the opposite edges.
 */
public class InitComputation extends
        BasicComputation<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, ConnectivityEdgeWritable> vertex,
            Iterable<ConnectivityMessage> messages) {

        if (getSuperstep() == 0) {
            // Communicate links to neighbors
            for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                if (!vertex.getId().equals(e.getTargetVertexId())) {
                    sendMessage(new IntWritable(e.getTargetVertexId().get()), new ConnectivityMessage(vertex.getId()));
                } else {
                    e.getValue().setValue(Integer.MAX_VALUE);
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
            for (MutableEdge<IntWritable, ConnectivityEdgeWritable> e : vertex.getMutableEdges()) {
                newNeighs.remove(e.getTargetVertexId().get());
            }

            // Add new neighbors
            for (int n : newNeighs) {
                vertex.addEdge(EdgeFactory.create(new IntWritable(n), new ConnectivityEdgeWritable(1, true, true)));
            }

        }
    }
}