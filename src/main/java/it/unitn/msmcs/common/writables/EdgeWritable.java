package it.unitn.msmcs.common.writables;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * State of the edge
 */
public class EdgeWritable implements Writable {
    private IntWritable size; // number of edges it represents
    private BooleanWritable isActive; // true if edge is currently used
    private BooleanWritable isReal; // true if it is part of the original graph

    /**
     * Empty constructor.
     */
    public EdgeWritable() {
        size = new IntWritable();
        isActive = new BooleanWritable();
        isReal = new BooleanWritable();
    }

    /**
     * Create a new edge state.
     * 
     * @param size     number of parallel edges it represents.
     * @param isReal   true if it represents an edge of the input graph.
     * @param isActive true if the edge is currently used in the computation.
     */
    public EdgeWritable(int size, boolean isReal, boolean isActive) {
        this.size = new IntWritable(size);
        this.isActive = new BooleanWritable(isActive);
        this.isReal = new BooleanWritable(isReal);

    }

    /**
     * @return number of parallel edges it represents.
     */
    public int getSize() {
        return size.get();
    }

    /**
     * @return true if the edge is currently used in the computation.
     */
    public boolean isActive() {
        return isActive.get();
    }

    /**
     * 
     * @return true if the edge is present in the original graph.
     */
    public boolean isReal() {
        return isReal.get();
    }

    /**
     * Set number of parallel edges it represents
     * 
     * @param size number of parallel edges.
     */
    public void setSize(int size) {
        this.size.set(size);
    }

    /**
     * Set if the edge is used in the computation
     * 
     * @param isActive true if it is used, false otherwise.
     */
    public void setIsActive(boolean isActive) {
        this.isActive.set(isActive);
    }

    /**
     * Read fields to generate the state.
     */
    public void readFields(DataInput in) throws IOException {
        size.readFields(in);
        isActive.readFields(in);
        isReal.readFields(in);
    }

    /**
     * Write fields to store the state.
     */
    public void write(DataOutput out) throws IOException {
        size.write(out);
        isActive.write(out);
        isReal.write(out);
    }

}
