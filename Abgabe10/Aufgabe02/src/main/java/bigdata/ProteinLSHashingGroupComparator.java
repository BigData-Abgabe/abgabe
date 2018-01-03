package bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ProteinLSHashingGroupComparator extends WritableComparator {
	public ProteinLSHashingGroupComparator() {
        super(TupleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleWritable t1 = (TupleWritable) a;
        TupleWritable t2 = (TupleWritable) b;

        int first = Integer.parseInt(t1.getList().get(0));
        int second = Integer.parseInt(t2.getList().get(0));
        return first < second ? -1 : first > second ? 1 : 0;
    }
}
