package bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MatrixMultiplicationSortComparator extends WritableComparator {

    public MatrixMultiplicationSortComparator() {
        super(TupleWritable.class, true);
    }

    //Sort the tuple by the third(last) entry of the matrix
    //Within a Reducer, the tuples should be sorted by the third Attribute of a tupel
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleWritable t1 = (TupleWritable) a;
        TupleWritable t2 = (TupleWritable) b;

        int result = t1.getList().get(0).compareTo(t2.getList().get(0));
        if (result != 0)
            return result;

        result = t1.getList().get(1).compareTo(t2.getList().get(1));
        if (result != 0)
            return result;

        //Secondary Sort by last element(third) of the tuple
        result = t1.getList().get(2).compareTo(t2.getList().get(2));
        return result;
    }
}