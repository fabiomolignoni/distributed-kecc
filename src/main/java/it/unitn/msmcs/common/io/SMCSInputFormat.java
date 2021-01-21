package it.unitn.msmcs.common.io;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;

public class SMCSInputFormat extends TextEdgeInputFormat<IntWritable, IntWritable> {
    /** Splitter for endpoints */

    @Override
    public EdgeReader<IntWritable, IntWritable> createEdgeReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new ConnectivityEdgeReader();
    }

    public class ConnectivityEdgeReader extends TextEdgeReaderFromEachLineProcessed<ArrayList<Integer>> {
        @Override
        protected ArrayList<Integer> preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split("\\D+");
            ArrayList<Integer> l = new ArrayList<Integer>();
            for (String t : tokens) {
                l.add(Integer.parseInt(t));
            }
            return l;
        }

        @Override
        protected IntWritable getSourceVertexId(ArrayList<Integer> endpoints) throws IOException {
            return new IntWritable(endpoints.get(0));
        }

        @Override
        protected IntWritable getTargetVertexId(ArrayList<Integer> endpoints) throws IOException {
            return new IntWritable(endpoints.get(1));
        }

        @Override
        protected IntWritable getValue(ArrayList<Integer> endpoints) throws IOException {
            return new IntWritable(endpoints.get(2));
        }
    }
}