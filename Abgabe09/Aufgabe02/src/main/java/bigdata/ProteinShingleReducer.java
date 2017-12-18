package bigdata;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProteinShingleReducer extends Reducer <Text,Text,NullWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	// vector of already written values to prevent doubling
        Vector<String> output = new Vector<String>();
        
        for (Text val : values) {
            String nextval = ( key.hashCode() + "\t" + val + "\t" + "1" );
            System.out.println(nextval);
            
            if (!output.contains(nextval)) {
                output.addElement(nextval);
                
                // Generating a final output
                Text outval = new Text(nextval);
                context.write(NullWritable.get(), outval);
            }
        }
    }
}

