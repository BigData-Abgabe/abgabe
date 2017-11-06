package bigdata;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MVMultReducer extends Reducer<Text, DoubleListWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleListWritable> values, Context context) throws IOException, InterruptedException {
	

		double result = 0;
        private List<Double> tmp = new ArrayList<Double>;

    	
    	// add all values of a row in values
    	for (DoubleListWritable value: values)
    		tmp=values.getList();
			for(Double d:tmp)
				result += d
    	
		context.write(key, new DoubleWritable(result));
		
    		     	
    }

}
