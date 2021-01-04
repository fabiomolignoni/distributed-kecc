package it.unitn.msmcs.common.writables;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EdgeStateWritable implements Writable {
    private IntWritable from;
    private IntWritable to;
    private IntWritable value;
    private BooleanWritable isActive;

    public EdgeStateWritable() {
        from = new IntWritable();
        to = new IntWritable();
        value = new IntWritable();
        isActive = new BooleanWritable();
    }

    public EdgeStateWritable(IntWritable from, IntWritable to, IntWritable value, BooleanWritable isActive) {
        this.from = from;
        this.to = to;
        this.value = value;
        this.isActive = isActive;
    }

    public EdgeStateWritable(EdgeStateWritable copy, boolean isActive) {
        this.from = new IntWritable(copy.getFrom().get());
        this.to = new IntWritable(copy.getTo().get());
        this.value = new IntWritable(copy.getValue().get());
        this.isActive = new BooleanWritable(isActive);
    }

    public IntWritable getFrom() {
        return from;
    }

    public IntWritable getTo() {
        return to;
    }

    public IntWritable getValue() {
        return value;
    }

    public BooleanWritable isActive() {
        return isActive;
    }

    public boolean isReal(IntWritable vertexId) {
        return from.equals(vertexId);
    }

    public void setValue(IntWritable value) {
        this.value.set(value.get());
    }

    public void setFrom(IntWritable from) {
        this.from.set(from.get());
    }

    public void setTo(IntWritable to) {
        this.to.set(to.get());
    }

    public void setIsActive(boolean isActive) {
        this.isActive.set(isActive);
    }

    public void readFields(DataInput in) throws IOException {
        from.readFields(in);
        to.readFields(in);
        value.readFields(in);
        isActive.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        from.write(out);
        to.write(out);
        value.write(out);
        isActive.write(out);
    }

}