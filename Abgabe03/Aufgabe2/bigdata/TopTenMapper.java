package bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, NullWritable, SortedMapWritable> {
    private Text v = new Text();
    private IntWritable k = new IntWritable();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	
    	v.set(itr.nextToken());
    	
    	// Return if line is missing cols
        if(!itr.hasMoreTokens())
        	return;
        
        k.set(Integer.parseInt(itr.nextToken()));
        
        SortedMapWritable map = new SortedMapWritable();
        map.put(k, v);
        
        // Since we'll only sort in the reducer, we only need one key: NullWritable
        // If we had more than one input file it would make sense to only save the top TOP_NUM from every input file
        context.write(NullWritable.get(), map);
    }
    

}
