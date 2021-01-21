package it.unitn.msmcs.common.io;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import it.unitn.msmcs.common.writables.ConnectivityEdgeWritable;

import java.io.IOException;

public class ConnectivityEdgeInputFormat extends TextEdgeInputFormat<IntWritable, ConnectivityEdgeWritable> {
    /** Splitter for endpoints */

    @Override
    public EdgeReader<IntWritable, ConnectivityEdgeWritable> createEdgeReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new ConnectivityEdgeReader();
    }

    public class ConnectivityEdgeReader extends TextEdgeReaderFromEachLineProcessed<IntPair> {
        @Override
        protected IntPair preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split("\\D+");
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
        protected ConnectivityEdgeWritable getValue(IntPair endpoints) throws IOException {
            return new ConnectivityEdgeWritable(1, true, true);
        }
    }
}