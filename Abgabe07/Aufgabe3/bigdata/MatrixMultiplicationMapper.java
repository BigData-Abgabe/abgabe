package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Mapper
 */
public class MatrixMultiplicationMapper extends Mapper<Text, TupleWritable, TupleWritable, TupleWritable> {

    private int rowsA; //Anzahl an Zeilen in A, wird über Kommandozeile übergeben
    private int columnsB; //Anzahl an Spalten in B, wird über Kommandozeile übergeben
    private String pathA; //Pfad von Matrix A
    private String pathB; //Pfad von Matrix B
    private TupleWritable writekey = new TupleWritable();
    private TupleWritable val = new TupleWritable();
    private ArrayList<String> keys = new ArrayList<String>();
    private ArrayList<String> values = new ArrayList<String>();

    
    //Wird nur 1 Mal aufgerufen, dient dafür dass alle Parameter über die Kommandozeilen eingelesen werden
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        rowsA = Integer.parseInt(conf.get("rows.A"));
        columnsB = Integer.parseInt(conf.get("columns.B"));
        pathA = conf.get("path.A");
        pathB = conf.get("path.B");
    }

    //Wie in der Vorlesung...
    @Override
    public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        if (key.toString().contains(pathA)) { //Eintrag kam aus Matrix A
            for (int k = 0 ; k < columnsB; k++) { //for k=0,...,l-1
                keys.clear(); values.clear();
                keys.add(value.getList().get(0)); //i
                keys.add(Integer.toString(k)); // k
                keys.add(value.getList().get(1)); //j
                values.add(value.getList().get(1)); //j
                values.add(value.getList().get(2)); // aij
                writekey.setList(keys);//key : (i,k,j) composite key, wobei secondary sort nach j
                val.setList(values); //value (j,aij)
                context.write(writekey, val);

            }
        }
        if (key.toString().contains(pathB)) { //Eintrag kam aus Matrix B
            for (int k = 0; k < rowsA; k++) { //for k =0,...,m-1
                keys.clear();values.clear();
                keys.add(Integer.toString(k)); //k
                keys.add(value.getList().get(1));//j
                keys.add(value.getList().get(0));//i
                values.add(value.getList().get(0));//i
                values.add(value.getList().get(2));//bij
                writekey.setList(keys); //key : (k,j,i) composite key, wobei secondary sort nach i
                val.setList(values); //value (i,bij)
                context.write(writekey, val);
            }
        }
    }
}