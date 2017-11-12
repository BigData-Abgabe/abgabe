package bigdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsReducer extends Reducer<NullWritable, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(NullWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> itr = values.iterator();
        float val = itr.next().get();
        float sum = val;
        int count = 1;
        float max = val;
        float min = val;
        
        
        // Process every value
        while(itr.hasNext()) {
        	val = itr.next().get();
            sum += val;
            ++count;
            if (val > max)
            	max = val;
            if (val < min)
            	min = val;
        }
        
        // Write out values
        context.write(new Text("Min"), new FloatWritable(min));
        context.write(new Text("Max"), new FloatWritable(max));
        context.write(new Text("Sum"), new FloatWritable(sum));
        context.write(new Text("Avg"), new FloatWritable(sum/count));
    }

}
