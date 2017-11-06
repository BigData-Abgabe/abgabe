package bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MVMultReducer extends Reducer<IntWritable, DoubleListWritable, IntWritable, DoubleWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<DoubleListWritable> values, Context context) throws IOException, InterruptedException {
	

		double result = 0;
		
		DoubleListWritable vector = new DoubleListWritable();
		String vectstring = context.getConfiguration().get("vector");
		StringTokenizer vectitr = new StringTokenizer(vectstring);
		while (vectitr.hasMoreTokens())
			vector.add(Double.parseDouble(vectitr.nextToken()));
    	
		for(DoubleListWritable value: values)
			for(int i = 0; i < vector.getList().size(); ++i)
				result += vector.getList().get(i).doubleValue() * value.getList().get(i);
    			
		context.write(key, new DoubleWritable(result));
		
    		     	
    }

}
