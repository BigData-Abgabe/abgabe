package bigdata;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenReducer extends Reducer<NullWritable, SortedMapWritable, IntWritable, Text> {

    @Override
    public void reduce(NullWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        
    	// Map from which we'll take the last TOP_NUM entries
    	SortedMapWritable map = new SortedMapWritable();
    	
    	// merge all SortedMapWritable in values
    	for (SortedMapWritable value: values)
    		map.putAll(value);
    	
    	// Another SortedMap to get the output sorted
    	SortedMapWritable topmap = new SortedMapWritable();
    	
    	// move the last TOP_NUM entries from map to topmap
    	for(int i = 0; i < bigdata.TopTen.TOP_NUM; ++i) {
    		topmap.put(map.lastKey(), map.get(map.lastKey()));
    		map.remove(map.lastKey());
    	}
    	
    	for (Entry<? super IntWritable, ? super Text> entry : topmap.entrySet())
    	     context.write((IntWritable) entry.getKey(), (Text) entry.getValue());
		
    		     	
    }

}
