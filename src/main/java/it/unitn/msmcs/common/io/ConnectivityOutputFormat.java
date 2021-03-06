package it.unitn.msmcs.common.io;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * Modifications of class
 * org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class ConnectivityOutputFormat<I extends WritableComparable, V extends Writable, E extends Writable>
        extends TextVertexOutputFormat<I, V, E> {

    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";

    /** Default split delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = AdjacencyListTextVertexInputFormat.LINE_TOKENIZE_VALUE_DEFAULT;

    @Override
    public AdjacencyListTextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new AdjacencyListTextVertexWriter();
    }

    /**
     * Vertex writer associated with {@link AdjacencyListTextVertexOutputFormat}.
     */
    protected class AdjacencyListTextVertexWriter extends TextVertexWriterToEachLine {
        /** Cached split delimeter */
        // private String delimiter = ",";

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
        }

        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {
            StringBuffer sb = new StringBuffer();

            Iterator<Edge<I, E>> itr = vertex.getEdges().iterator();
            int id = ((IntWritable) vertex.getId()).get();
            boolean first = true;
            while (itr.hasNext()) {
                Edge<I, E> e = itr.next();
                int target = ((IntWritable) e.getTargetVertexId()).get();
                if (id < target) {
                    String start = first ? "" : "\n";
                    sb.append(start + id + "," + target);
                    first = false;
                }
            }

            return new Text(sb.toString());
        }
    }

}