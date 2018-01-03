package bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ProteinLSHashingSortComparator extends WritableComparator {
	public ProteinLSHashingSortComparator() {
        super(TupleWritable.class, true);
    }
	
	@Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleWritable t1 = (TupleWritable) a;
        TupleWritable t2 = (TupleWritable) b;

        int first = Integer.parseInt(t1.getList().get(0));
        int second = Integer.parseInt(t2.getList().get(0));
        int result = first < second ? -1 : first > second ? 1 : 0;
        if (result != 0)
            return result;

        result = t1.getList().get(1).compareTo(t2.getList().get(1));
        if (result != 0)
            return result;

        first = Integer.parseInt(t1.getList().get(2));
        second = Integer.parseInt(t2.getList().get(2));
        return first < second ? -1 : first > second ? 1 : 0;
    }
}
