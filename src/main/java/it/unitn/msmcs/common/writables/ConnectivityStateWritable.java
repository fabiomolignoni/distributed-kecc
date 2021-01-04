package it.unitn.msmcs.common.writables;

import org.apache.hadoop.io.BooleanWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConnectivityStateWritable implements Writable {
    private BooleanWritable isLast;
    private BooleanWritable isMerged;
    private IntWritable subgraph;
    private IntWritable mergeTarget;
    private IntWritable nActiveEdges;

    public ConnectivityStateWritable() {
        isLast = new BooleanWritable(false);
        isMerged = new BooleanWritable(false);
        subgraph = new IntWritable();
        mergeTarget = new IntWritable(-1);
        nActiveEdges = new IntWritable(0);
    }

    public BooleanWritable isLast() {
        return isLast;
    }

    public BooleanWritable isMerged() {
        return isMerged;
    }

    public IntWritable getSubgraph() {
        return subgraph;
    }

    public IntWritable getMergeTarget() {
        return mergeTarget;
    }

    public IntWritable getNumberActiveEdges() {
        return nActiveEdges;
    }

    public void setIsLast(boolean isLast) {
        this.isLast.set(isLast);
    }

    public void setIsMerged(boolean isMerged) {
        this.isMerged.set(isMerged);
    }

    public void setNumberActiveEdges(int nActiveEdges) {
        this.nActiveEdges.set(nActiveEdges);
    }

    public void decreaseNumberActiveEdges() {
        nActiveEdges.set(nActiveEdges.get() - 1);
    }

    public void increaseNumberActiveEdges() {
        nActiveEdges.set(nActiveEdges.get() + 1);
    }

    public void setSubgraph(IntWritable subgraph) {
        this.subgraph.set(subgraph.get());
    }

    public void setMergeTarget(IntWritable target) {
        this.mergeTarget.set(target.get());
    }

    public boolean updateSubgraph(IntWritable subgraph) {
        if (this.subgraph.compareTo(subgraph) > 0) {
            this.subgraph.set(subgraph.get());
            return true;
        } else {
            return false;
        }
    }

    public void readFields(DataInput in) throws IOException {
        isLast.readFields(in);
        isMerged.readFields(in);
        subgraph.readFields(in);
        mergeTarget.readFields(in);
        nActiveEdges.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        isLast.write(out);
        isMerged.write(out);
        subgraph.write(out);
        mergeTarget.write(out);
        nActiveEdges.write(out);
    }
}