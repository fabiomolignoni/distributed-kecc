package it.unitn.msmcs.common.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * A Writable Map.
 * 
 * Minor modifications of org.apache.hadoop.io.MapWritable
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SubgraphInfoMapWritable extends AbstractMapWritable implements Map<IntWritable, SubgraphInfoWritable> {

    private Map<IntWritable, SubgraphInfoWritable> instance;

    /** Default constructor. */
    public SubgraphInfoMapWritable() {
        super();
        this.instance = new HashMap<IntWritable, SubgraphInfoWritable>();
    }

    public void clear() {
        instance.clear();
    }

    public boolean containsKey(Object key) {
        return instance.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return instance.containsValue(value);
    }

    public Set<Map.Entry<IntWritable, SubgraphInfoWritable>> entrySet() {
        return instance.entrySet();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof SubgraphInfoMapWritable) {
            SubgraphInfoMapWritable map = (SubgraphInfoMapWritable) obj;
            if (size() != map.size()) {
                return false;
            }

            return entrySet().equals(map.entrySet());
        }

        return false;
    }

    public SubgraphInfoWritable get(Object key) {
        return instance.get(key);
    }

    public boolean isEmpty() {
        return instance.isEmpty();
    }

    public Set<IntWritable> keySet() {
        return instance.keySet();
    }

    public SubgraphInfoWritable put(IntWritable key, SubgraphInfoWritable value) {
        addToMap(key.getClass());
        addToMap(value.getClass());
        return instance.put(key, value);
    }

    public void putAll(Map<? extends IntWritable, ? extends SubgraphInfoWritable> t) {
        for (Map.Entry<? extends IntWritable, ? extends SubgraphInfoWritable> e : t.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    public SubgraphInfoWritable remove(Object key) {
        return instance.remove(key);
    }

    public int size() {
        return instance.size();
    }

    public Collection<SubgraphInfoWritable> values() {
        return instance.values();
    }

    // Writable

    @Override
    public void write(DataOutput out) throws IOException {
        // Write out the number of entries in the map

        out.writeInt(instance.size());

        // Then write out each key/value pair

        for (Map.Entry<IntWritable, SubgraphInfoWritable> e : instance.entrySet()) {
            e.getKey().write(out);
            e.getValue().write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        // First clear the map. Otherwise we will just accumulate
        // entries every time this method is called.
        this.instance.clear();

        // Read the number of entries in the map

        int entries = in.readInt();
        // Then read each key/value pair

        for (int i = 0; i < entries; i++) {
            IntWritable key = new IntWritable();
            key.readFields(in);

            SubgraphInfoWritable value = new SubgraphInfoWritable();
            value.readFields(in);
            instance.put(key, value);
        }
    }
}
