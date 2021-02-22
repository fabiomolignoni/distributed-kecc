package it.unitn.msmcs.kecc;

import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

/**
 * If the input graph is undirected it adds the opposite edges.
 */
public class InitComputation extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        if (getSuperstep() == 0) {

            // Send to all neighbors your id
            sendMessageToAllEdges(vertex, new ConnMessage(vertex.getId(), new IntWritable(-1)));
        } else {
            // Read all the received messages
            HashSet<Integer> neighs = new HashSet<Integer>();
            for (ConnMessage m : messages) {
                neighs.add(m.getSender().get());
            }

            // Remove already-known neighbors
            for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                neighs.remove(e.getTargetVertexId().get());
            }

            // Initialize unknown neighbors
            for (Integer n : neighs) {
                vertex.addEdge(EdgeFactory.create(new IntWritable(n), new EdgeWritable(1, true, true)));
            }
        }
    }
}