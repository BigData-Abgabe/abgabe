package bigdata;

import org.apache.hadoop.mapreduce.Partitioner;


public class MatrixMultiplicationPartitioner extends Partitioner<TupleWritable, TupleWritable> {

	//Partioniere über die ersten beiden Werte eines Tuples, genau wie beim Gruppieren
	//Somit sorgen wir dafür dass die Einträge mit dem gleichen natural Key in einen Reducer gelangen
    @Override
    public int getPartition(TupleWritable tupleWritable, TupleWritable tupleWritable2, int i) {
    	//gibt i,j als Zahl aus:
    	//Beispiel: i = 1 , j= 3 würde 13 ausgeben, i = 3, j = 1 würde 31 ausgeben
        return Integer.parseInt(tupleWritable.getList().get(0) + tupleWritable.getList().get(1));
    }
}