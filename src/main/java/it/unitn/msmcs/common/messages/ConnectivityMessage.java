package it.unitn.msmcs.common.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class ConnectivityMessage implements Writable {
    IntWritable sender;
    IntWritable content;

    public ConnectivityMessage() {
        sender = new IntWritable();
        content = new IntWritable();
    }

    public ConnectivityMessage(IntWritable sender) {
        this.sender = new IntWritable(sender.get());
        this.content = new IntWritable();
    }

    public ConnectivityMessage(IntWritable sender, IntWritable content) {
        this.sender = new IntWritable(sender.get());
        this.content = new IntWritable(content.get());
    }

    public IntWritable getSender() {
        return sender;
    }

    public IntWritable getContent() {
        return content;
    }

    public void setContent(IntWritable content) {
        this.content = content;
    }

    public void write(DataOutput out) throws IOException {
        sender.write(out);
        content.write(out);

    }

    public void readFields(DataInput in) throws IOException {
        sender.readFields(in);
        content.readFields(in);

    }

}
