package it.unitn.msmcs.common.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * State of the message
 */
public class ConnMessage implements Writable {
    IntWritable sender; // sender of the message
    IntWritable content; // content of the message

    /**
     * Empty constructor.
     */
    public ConnMessage() {
        sender = new IntWritable();
        content = new IntWritable();
    }

    /**
     * Create a new message.
     * 
     * @param sender  ID of the sender
     * @param content content of the message
     */
    public ConnMessage(IntWritable sender, IntWritable content) {
        this.sender = sender;
        this.content = content;
    }

    /**
     * Get the sender of the message
     * 
     * @return ID of the sender
     */
    public IntWritable getSender() {
        return sender;
    }

    /**
     * Get the content of the message.
     * 
     * @return integer content of the message.
     */
    public IntWritable getContent() {
        return content;
    }

    /**
     * Load the message in memory
     */
    public void readFields(DataInput in) throws IOException {
        sender.readFields(in);
        content.readFields(in);

    }

    /**
     * Store the message.
     */
    public void write(DataOutput out) throws IOException {
        sender.write(out);
        content.write(out);

    }

}
