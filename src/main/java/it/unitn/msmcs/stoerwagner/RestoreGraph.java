package it.unitn.msmcs.connectivity;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.HashSet;

import it.unitn.msmcs.common.messages.ConnectivityMessage;
import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;

public class RestoreGraph
        extends BasicComputation<IntWritable, ConnectivityStateWritable, EdgeStateWritable, ConnectivityMessage> {

    @Override
    public void compute(Vertex<IntWritable, ConnectivityStateWritable, EdgeStateWritable> vertex,
            Iterable<ConnectivityMessage> messages) {
        ArrayList<Edge<IntWritable, EdgeStateWritable>> edges = new ArrayList<Edge<IntWritable, EdgeStateWritable>>();

        int nActiveEdges = 0;
        HashSet<Integer> created = new HashSet<Integer>();

        for (MutableEdge<IntWritable, EdgeStateWritable> e : vertex.getMutableEdges()) {
            EdgeStateWritable edgeState = e.getValue();
            if (edgeState.isReal(vertex.getId()) && !created.contains(edgeState.getTo().get())) {
                created.add(e.getTargetVertexId().get());
                edges.add(EdgeFactory.create(new IntWritable(edgeState.getTo().get()),
                        new EdgeStateWritable(edgeState, edgeState.getValue().get() == 0)));
                if (edgeState.getValue().get() == 0) {
                    nActiveEdges += 1;
                }
            }
        }

        vertex.setEdges(edges);
        vertex.getValue().setNumberActiveEdges(nActiveEdges);
        if (nActiveEdges != 0) {
            aggregate(ConnectivityMaster.CONTINUE_COMPUTATION, new BooleanWritable(true));
        } else {
            vertex.voteToHalt();

        }
    }
}