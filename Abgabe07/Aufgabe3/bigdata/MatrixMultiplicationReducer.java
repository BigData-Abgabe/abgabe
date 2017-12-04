package bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MatrixMultiplicationReducer extends Reducer<TupleWritable, TupleWritable, Text, DoubleWritable> {

    private StringBuilder sb = new StringBuilder();
    private Text k = new Text();
    private DoubleWritable val = new DoubleWritable(); //entry of the result matrix

    //Die Tupel werden gruppiert über die 
    @Override
    public void reduce(TupleWritable key, Iterable<TupleWritable> values, Context context)
            throws IOException, InterruptedException {
        double result = 0d;
        int j1;
        double entry1;
        int j2 = 0;
        double entry2 = 0d;
        //Für jedes Tupel, multipliziere A[j] * B[j]
        //Man multipliziere je zwei benachtbarte Tupel aus der Liste miteinander und addiert sie zum Result.
        //Zwei benachtbarte Tupel stammen IMMER aus unterschiedlichen Matrizen!
        for (TupleWritable t : values) { //
            j1 = Integer.parseInt(t.getList().get(0)); //j
            entry1 = Double.parseDouble(t.getList().get(1)); //aij/bjk
            if (j1 == j2) { //only multiply if they have the same j
                result += entry1 * entry2;
            }
            j2 = j1;
            entry2 = entry1;
        }
        if (result != 0) {
            sb.setLength(0);
            sb.append(key.getList().get(0)).append("\t").append(key.getList().get(1)).append("\t");
            k.set(sb.toString()); //(i,k) Ausgabe soll die i-te Zeile, k-te Spalte von C 
            val.set(result); //Und das Ergebnis von C
            context.write(k, val); //emit(i,k),r)
        }

    }
}