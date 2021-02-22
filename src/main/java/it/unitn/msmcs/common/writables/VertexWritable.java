package it.unitn.msmcs.common.writables;

import org.apache.giraph.utils.ArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * State of the vertex.
 */
public class VertexWritable implements Writable {
    ArrayWritable<IntWritable> contacted; // Contains the contacted vertices
    IntWritable mergeTarget; // representative vertex

    /**
     * Empty constructor.
     */
    public VertexWritable() {
        contacted = new ArrayWritable<IntWritable>();
        mergeTarget = new IntWritable(-1);
    }

    /**
     * Get the contacted vertices.
     * 
     * @return list containing all the contacted vertices.
     */
    public ArrayList<IntWritable> getContacted() {
        ArrayList<IntWritable> res = new ArrayList<IntWritable>();
        if (contacted != null && contacted.get() != null)
            Collections.addAll(res, contacted.get());
        return res;
    }

    /**
     * Get the node with whom it merged
     * 
     * @return the representative ID
     */
    public IntWritable getMergeTarget() {
        return mergeTarget;
    }

    /**
     * Change the contacted array
     * 
     * @param values the new contacted array
     */
    public void setContacted(ArrayList<IntWritable> values) {
        contacted.set(values.toArray(new IntWritable[0]));
    }

    /**
     * Set a new representative for the vertex.
     * 
     * @param mergeTarget representative ID
     */
    public void setMergeTarget(IntWritable mergeTarget) {
        this.mergeTarget = mergeTarget;
    }

    /**
     * Delete all entries in the contacted array
     */
    public void clearContacted() {
        contacted.set(new IntWritable[0]);
    }

    /**
     * Check if the vertex has no representative
     * 
     * @return true if the vertex is still active in the computation
     */
    public boolean isActive() {
        return mergeTarget.get() == -1;
    }

    /**
     * Load the state
     */
    public void readFields(DataInput in) throws IOException {
        contacted.readFields(in);
        mergeTarget.readFields(in);
    }

    /**
     * Store the state
     */
    public void write(DataOutput out) throws IOException {
        contacted.write(out);
        mergeTarget.write(out);
    }
}