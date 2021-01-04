package it.unitn.msmcs.common.writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SubgraphInfoWritable implements Writable {
    private IntWritable tightValue;
    private IntWritable id;

    public SubgraphInfoWritable() {
        tightValue = new IntWritable();
        id = new IntWritable();
    }

    public SubgraphInfoWritable(IntWritable id, int value) {
        this.tightValue = new IntWritable(value);
        this.id = id;
    }

    public IntWritable getTightValue() {
        return tightValue;
    }

    public IntWritable getId() {
        return id;
    }

    public void setTightValue(IntWritable tightValue) {
        this.tightValue.set(tightValue.get());
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        tightValue.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        id.write(out);
        tightValue.write(out);

    }
}