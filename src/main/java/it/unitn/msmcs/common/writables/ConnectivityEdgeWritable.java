package it.unitn.msmcs.common.writables;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConnectivityEdgeWritable implements Writable {
    private IntWritable value;
    private BooleanWritable isActive;
    private BooleanWritable isReal;

    public ConnectivityEdgeWritable() {
        value = new IntWritable();
        isActive = new BooleanWritable();
        isReal = new BooleanWritable();
    }

    public ConnectivityEdgeWritable(int value, boolean isReal, boolean isActive) {
        this.value = new IntWritable(value);
        this.isActive = new BooleanWritable(isActive);
        this.isReal = new BooleanWritable(isReal);

    }

    public int getValue() {
        return value.get();
    }

    public boolean isActive() {
        return isActive.get();
    }

    public boolean isReal() {
        return isReal.get();
    }

    public void setValue(int value) {
        this.value.set(value);
    }

    public void setIsReal(boolean isReal) {
        this.isReal.set(isReal);
    }

    public void setIsActive(boolean isActive) {
        this.isActive.set(isActive);
    }

    public void readFields(DataInput in) throws IOException {
        value.readFields(in);
        isActive.readFields(in);
        isReal.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        value.write(out);
        isActive.write(out);
        isReal.write(out);
    }

}
