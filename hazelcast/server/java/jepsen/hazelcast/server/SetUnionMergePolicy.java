package jepsen.hazelcast_server;

import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.core.EntryView;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;

import java.io.IOException;
import java.util.TreeSet;

public class SetUnionMergePolicy implements MapMergePolicy, DataSerializable {
  @Override
  public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
    // Merge long arrays as sets
    final long[] a1;
    final long[] a2;
    if (null == mergingEntry.getValue()) {
      a1 = new long[0];
    } else {
      a1 = (long[]) mergingEntry.getValue();
    }
    if (null == existingEntry.getValue()) {
      a2 = new long[0];
    } else {
      a2 = (long[]) existingEntry.getValue();
    }

    // Merge arrays
    final TreeSet<Long> merged = new TreeSet<Long>();

    int i;
    for (i = 0; i < a1.length; i++) {
      merged.add(a1[i]);
    }
    for (i = 0; i < a2.length; i++) {
      merged.add(a2[i]);
    }

    System.out.println("MERGE RESULT: " + merged.toString());

    // Convert back to long array
    final long[] m = new long[merged.size()];
    i = 0;
    for (Long element : merged) {
      m[i] = element;
      i++;
    }

    return m;
  }

  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
  }

  @Override
  public void readData(ObjectDataInput in) throws IOException {
  }
}
