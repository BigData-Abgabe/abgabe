package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MVMultMapper extends Mapper<Object, Text, Text, DoubleListWritable> {
    private Text key = new Text();
    private DoubleListWritable value = new DoubleListWritable();
	private double tmp; 
	private Configuration conf= context.getConfiguration();
	private StringTokenizer vector = new StringTokenizer(conf.get("vector")) //get vector element
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
        
        // First col as key
        key = itr.nextToken();
		// vector name is not needed
		vector.nextToken();
		
        if(!itr.hasMoreTokens())
        	return;
        
        // Other cols as value
       
        while (!itr.hasMoreTokens())
			// mutiply one row of the matrix with the vector and save row into list
			value.add(Double.parseDouble(itr.nextToken())*Double.parseDouble(vector.nextToken()));
		 
		context.write(key, value);	
        	

    }

}
