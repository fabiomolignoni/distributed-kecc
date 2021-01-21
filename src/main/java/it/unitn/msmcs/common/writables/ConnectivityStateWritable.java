package it.unitn.msmcs.common.writables;

import org.apache.giraph.utils.ArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class ConnectivityStateWritable implements Writable {
    ArrayWritable<IntWritable> contacted;
    IntWritable mergeTarget;

    public ConnectivityStateWritable() {
        contacted = new ArrayWritable<IntWritable>();
        mergeTarget = new IntWritable(-1);
    }

    public IntWritable[] getContacted() {
        return contacted.get();
    }

    public IntWritable getMergeTarget() {
        return mergeTarget;
    }

    public void setContacted(ArrayList<IntWritable> values) {
        contacted.set(values.toArray(new IntWritable[0]));
    }

    public void setContacted(IntWritable value) {
        contacted.set(new IntWritable[] { value });
    }

    public void setMergeTarget(IntWritable target) {
        this.mergeTarget = target;
    }

    public void clearContacted() {
        contacted.set(new IntWritable[0]);
    }

    public boolean isActive() {
        return mergeTarget.get() == -1;
    }

    public void readFields(DataInput in) throws IOException {
        contacted.readFields(in);
        mergeTarget.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        contacted.write(out);
        mergeTarget.write(out);
    }
}