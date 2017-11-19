package bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatisticsMapper extends Mapper<Object, Text, NullWritable, MapWritable> {
    private IntWritable col = new IntWritable();
    private FloatWritable fvalue = new FloatWritable();
    private static final FloatWritable ONE = new FloatWritable(1);
    
    private MapWritable map = new MapWritable();
    
    private static final Text MAX = new Text("Max");
    private static final Text MIN = new Text("Min");
    private static final Text SUM = new Text("Sum");
    private static final Text COUNTER = new Text("Counter");
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	col.set(conf.getInt("stats.col", 1));
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
        
        // map tuple
        int counter = 1;
        while (itr.hasMoreTokens()) {
        	fvalue.set(Float.parseFloat(itr.nextToken()));
            if (col.get() == counter) {
                map.put(MIN, fvalue);
                map.put(MAX, fvalue);
                map.put(SUM, fvalue);
                map.put(COUNTER, ONE);
                context.write(NullWritable.get(), map);
                break;
            }
            counter++;
        }


    }

}
