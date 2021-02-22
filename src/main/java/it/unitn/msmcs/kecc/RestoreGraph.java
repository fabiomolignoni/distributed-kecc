package it.unitn.msmcs.kecc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
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
            HashSet<Integer> input = getInput(getConf().get("input.vertices"));
            if (input.contains(vertex.getId().get())) {
                aggregate(ConnectivityMaster.MINID, state.getMergeTarget());
            }
        } else if (round == 1) {
            HashSet<Integer> input = getInput(getConf().get("input.vertices"));
            int minId = ((IntWritable) getAggregatedValue(ConnectivityMaster.MINID)).get();

            if (input.contains(vertex.getId().get())) {
                // An input vertex is not anymore in the same subgraph
                if (state.getMergeTarget().get() != minId) {
                    aggregate(ConnectivityMaster.STOP, new BooleanWritable(true));
                }

            } else if (input.size() == 1 && vertex.getId().get() != minId && state.getMergeTarget().get() == minId) {
                // the 1 input is not disconnected
                aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
            }

            // Start the restore process
            for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                if (e.getValue().isReal()) {
                    sendMessage(new IntWritable(e.getTargetVertexId().get()),
                            new ConnMessage(vertex.getId(), state.getMergeTarget()));
                }
            }
        } else if (round == 2) {
            boolean contComp = ((BooleanWritable) getAggregatedValue(ConnectivityMaster.CONTINUE)).get();
            boolean stopComp = ((BooleanWritable) getAggregatedValue(ConnectivityMaster.STOP)).get();
            HashSet<Integer> input = getInput(getConf().get("input.vertices"));
            int minId = ((IntWritable) getAggregatedValue(ConnectivityMaster.MINID)).get();

            if ((input.size() == 1 && contComp) || (input.size() > 1 && !stopComp)) {
                if (state.getMergeTarget().get() == minId) {
                    aggregate(ConnectivityMaster.CONTINUE, new BooleanWritable(true));
                    HashSet<Integer> same = new HashSet<Integer>();
                    ArrayList<Edge<IntWritable, EdgeWritable>> edges = new ArrayList<Edge<IntWritable, EdgeWritable>>();

                    for (ConnMessage m : messages) {
                        if (m.getContent().equals(state.getMergeTarget())) {
                            same.add(m.getSender().get());
                        }
                    }

                    for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                        if (e.getValue().isReal() && same.contains(e.getTargetVertexId().get())
                                && e.getValue().getSize() > 0) {
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
                } else {
                    try {
                        removeVertexRequest(vertex.getId());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            } else {
                ArrayList<Edge<IntWritable, EdgeWritable>> edges = new ArrayList<Edge<IntWritable, EdgeWritable>>();
                for (Edge<IntWritable, EdgeWritable> e : vertex.getEdges()) {
                    if (e.getValue().isReal()) {
                        edges.add(EdgeFactory.create(new IntWritable(e.getTargetVertexId().get()),
                                new EdgeWritable(1, true, true)));
                    }
                }

                vertex.voteToHalt();
            }
        }

        if (round == 0) {

        } else {

        }
    }

    private HashSet<Integer> getInput(String input) {
        HashSet<Integer> list = new HashSet<Integer>();
        String[] values = input.split("-");
        for (String s : values) {
            list.add(Integer.parseInt(s));
        }

        return list;
    }
}