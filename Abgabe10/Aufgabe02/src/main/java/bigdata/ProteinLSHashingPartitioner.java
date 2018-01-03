package bigdata;

import org.apache.hadoop.mapreduce.Partitioner;

import edu.umd.cloud9.io.pair.PairOfStringInt;

public class ProteinLSHashingPartitioner extends Partitioner<TupleWritable, PairOfStringInt> {

    @Override
    public int getPartition(TupleWritable tupleWritable, PairOfStringInt pairOfStringInt, int i) {
        return Integer.parseInt(tupleWritable.getList().get(0)) % i;
    }
}
