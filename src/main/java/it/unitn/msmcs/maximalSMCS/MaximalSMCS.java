package it.unitn.msmcs.maximalSMCS;

import java.util.Arrays;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

public class MaximalSMCS extends BasicComputation<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    public void compute(Vertex<IntWritable, IntWritable, IntWritable> vertex, Iterable<IntWritable> messages) {
        if (getSuperstep() == 0) {
            List<String> input = Arrays.asList(getConf().get("input.vertices").split("-"));
            if (input.contains(vertex.getId().toString())) {
                int connectivity = 0;
                for (Edge<IntWritable, IntWritable> e : vertex.getEdges()) {
                    connectivity = Math.max(connectivity, e.getValue().get());
                }
                vertex.setValue(new IntWritable(connectivity));
                sendToAllKConnectedNeighbors(vertex, connectivity);
            }
        } else {
            int original = vertex.getValue().get();
            for (IntWritable m : messages) {
                if (m.compareTo(vertex.getValue()) > 0) {
                    vertex.getValue().set(m.get());
                }
            }

            if (original != vertex.getValue().get()) {
                sendToAllKConnectedNeighbors(vertex, vertex.getValue().get());
            }
        }

        vertex.voteToHalt();
    }

    private void sendToAllKConnectedNeighbors(Vertex<IntWritable, IntWritable, IntWritable> vertex, int connectivity) {
        for (Edge<IntWritable, IntWritable> e : vertex.getEdges()) {
            if (e.getValue().get() >= connectivity) {
                sendMessage(e.getTargetVertexId(), new IntWritable(connectivity));
            }
        }
    }
}