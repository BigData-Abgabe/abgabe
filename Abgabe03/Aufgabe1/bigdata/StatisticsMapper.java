package bigdata;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StatisticsMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text v1 = new Text("1");
    private Text v2 = new Text("2");
    private FloatWritable fvalue = new FloatWritable();
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
        
        // Second col is v1
        fvalue.set(Float.parseFloat(itr.nextToken()));
        context.write(v1, fvalue);

        if(!itr.hasMoreTokens())
        	return;
        
        // Third col is v2
        fvalue.set(Float.parseFloat(itr.nextToken()));
        context.write(v2, fvalue);


    }

}
