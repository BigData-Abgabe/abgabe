package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StatisticsMapper extends Mapper<Object, Text, NullWritable, FloatWritable> {
    private int col;
    private FloatWritable fvalue = new FloatWritable();
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	col = conf.getInt("stats.col", 1);
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String valString = value.toString();
        if(valString.startsWith("#"))
            // This line is a comment
            return;
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        // Return if line is missing cols
        if(!itr.hasMoreTokens())
        	return;
        
        // Ignore first col (name)
        itr.nextToken();

        if(!itr.hasMoreTokens())
        	return;
        
        // Second col is v1; use it if col=1
        if (col == 1) {
        	fvalue.set(Float.parseFloat(itr.nextToken()));
        	context.write(NullWritable.get(), fvalue);
        	return;
        }

        if(!itr.hasMoreTokens())
        	return;
        
        // Third col is v2; use it if col=2
        fvalue.set(Float.parseFloat(itr.nextToken()));
        context.write(NullWritable.get(), fvalue);


    }

}
