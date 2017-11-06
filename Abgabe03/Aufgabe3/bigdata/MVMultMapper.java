package bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MVMultMapper extends Mapper<Text, Text, IntWritable, DoubleListWritable> {
    @Override
    public void map(Text k, Text val, Context context) throws IOException, InterruptedException {
    	
    	DoubleListWritable value = new DoubleListWritable();
    	
    	if(k.toString().equalsIgnoreCase("V")) {
    		// This is our vector
    		context.getConfiguration().set("vector", val.toString());
    		return;
    	}
    	
    	
    	StringTokenizer line = new StringTokenizer(k.toString());
       
        while (line.hasMoreTokens())
			value.add(Double.parseDouble(line.nextToken()));
        
		context.write(new IntWritable(Integer.parseInt(k.toString())), value);	
        	

    }

}
