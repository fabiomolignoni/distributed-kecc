package it.unitn.msmcs.common.io;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import it.unitn.msmcs.common.writables.EdgeStateWritable;

import java.io.IOException;
import java.util.regex.Pattern;

public class ConnectivityEdgeInputFormat extends TextEdgeInputFormat<IntWritable, EdgeStateWritable> {
    /** Splitter for endpoints */
    private static final Pattern SEPARATOR = Pattern.compile(",");

    @Override
    public EdgeReader<IntWritable, EdgeStateWritable> createEdgeReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new ConnectivityEdgeReader();
    }

    public class ConnectivityEdgeReader extends TextEdgeReaderFromEachLineProcessed<IntPair> {
        @Override
        protected IntPair preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            return new IntPair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
        }

        @Override
        protected IntWritable getSourceVertexId(IntPair endpoints) throws IOException {
            return new IntWritable(endpoints.getFirst());
        }

        @Override
        protected IntWritable getTargetVertexId(IntPair endpoints) throws IOException {
            return new IntWritable(endpoints.getSecond());
        }

        @Override
        protected EdgeStateWritable getValue(IntPair endpoints) throws IOException {
            return new EdgeStateWritable(new IntWritable(endpoints.getFirst()), new IntWritable(endpoints.getSecond()),
                    new IntWritable(0), new BooleanWritable(true));
        }
    }
}