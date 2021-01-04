package it.unitn.msmcs.common.io;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import it.unitn.msmcs.common.writables.ConnectivityStateWritable;
import it.unitn.msmcs.common.writables.EdgeStateWritable;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class ConnectivityVertexInputFormat
        extends TextVertexInputFormat<IntWritable, ConnectivityStateWritable, EdgeStateWritable> {
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new IntIntNullVertexReader();
    }

    public class IntIntNullVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {
        private IntWritable id;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            id = new IntWritable(Integer.parseInt(tokens[0]));
            return tokens;
        }

        @Override
        protected IntWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected ConnectivityStateWritable getValue(String[] tokens) throws IOException {
            return new ConnectivityStateWritable();
        }

        @Override
        protected Iterable<Edge<IntWritable, EdgeStateWritable>> getEdges(String[] tokens) throws IOException {
            List<Edge<IntWritable, EdgeStateWritable>> edges = Lists.newArrayListWithCapacity(tokens.length - 1);
            for (int n = 1; n < tokens.length; n++) {
                IntWritable from = new IntWritable(Integer.parseInt(tokens[0]));
                IntWritable to = new IntWritable(Integer.parseInt(tokens[n]));
                edges.add(EdgeFactory.create(to,
                        new EdgeStateWritable(from, to, new IntWritable(0), new BooleanWritable(true))));
            }
            return edges;
        }
    }
}