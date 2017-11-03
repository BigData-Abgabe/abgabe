package bigdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> itr = values.iterator();
        float val = itr.next().get();
        float sum = val;
        int count = 1;
        float max = val;
        
        
        // Process every value
        while(itr.hasNext()) {
        	val = itr.next().get();
            sum += val;
            ++count;
            if (val > max)
            	max = val;
        }
        
        // Write out values
        context.write(new Text("Max_"  + key.toString()), new FloatWritable(max));
        context.write(new Text("Sum_" + key.toString()), new FloatWritable(sum));
        context.write(new Text("Avg_" + key.toString()), new FloatWritable(sum/count));
    }

}
