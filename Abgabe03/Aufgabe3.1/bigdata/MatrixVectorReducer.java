package bigdata;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixVectorReducer extends Reducer<Text, DoubleListWritable, Text, DoubleWritable> {
	@Override
    public void reduce(Text key, Iterable<DoubleListWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		Iterator<DoubleListWritable> itr = values.iterator();
		while(itr.hasNext()) {
			DoubleListWritable w = itr.next();
			for (Double d : w.getList()) {
				sum+=d;
			}	
		}
        context.write(key, new DoubleWritable(sum));
	}
}
