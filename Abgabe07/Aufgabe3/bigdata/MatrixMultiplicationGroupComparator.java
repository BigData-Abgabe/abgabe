package bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MatrixMultiplicationGroupComparator extends WritableComparator {

    public MatrixMultiplicationGroupComparator() {
        super(TupleWritable.class,true);
    }

    //Group a tuple by the first two entries of the matrix (go to the same reduce call)
    //All tuples that have the same first two attributes should belong to the same Reducer! (Natural key)
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleWritable t1 = (TupleWritable) a;
        TupleWritable t2 = (TupleWritable) b;

        int result = t1.getList().get(0).compareTo(t2.getList().get(0));
        if(result != 0)
            return result;

        result = t1.getList().get(1).compareTo(t2.getList().get(1));
        return result;
    }
}