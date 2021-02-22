package it.unitn.msmcs.kecc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import it.unitn.msmcs.common.messages.ConnMessage;
import it.unitn.msmcs.common.writables.EdgeWritable;
import it.unitn.msmcs.common.writables.VertexWritable;

public class RestoreGraph extends BasicComputation<IntWritable, VertexWritable, EdgeWritable, ConnMessage> {

    @Override
    public void compute(Vertex<IntWritable, VertexWritable, EdgeWritable> vertex, Iterable<ConnMessage> messages) {
        int round = ((IntWritable) getAggregatedValue(ConnectivityMaster.ROUND)).get();
        VertexWritable state = vertex.getValue();

        if (round == 0) {
            for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().isReal()) {
                    sendMessage(new IntWritable(e.getTargetVertexId().get()),
                            new ConnMessage(vertex.getId(), state.getMergeTarget()));
                }
            }
        } else {
            HashSet<Integer> same = new HashSet<Integer>();
            ArrayList<Edge<IntWritable, EdgeWritable>> edges = new ArrayList<Edge<IntWritable, EdgeWritable>>();

            for (ConnMessage m : messages) {
                if (m.getContent().equals(state.getMergeTarget())) {
                    same.add(m.getSender().get());
                }
            }

            for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().isReal() && same.contains(e.getTargetVertexId().get()) && e.getValue().getSize() > 0) {
                    edges.add(EdgeFactory.create(new IntWritable(e.getTargetVertexId().get()),
                            new EdgeWritable(1, true, true)));
                }
            }

            vertex.setEdges(edges);
            state.setMergeTarget(new IntWritable(-1));
            state.clearContacted();
            if (edges.size() == 0) {
                try {
                    removeVertexRequest(vertex.getId());
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

        }
    }
}