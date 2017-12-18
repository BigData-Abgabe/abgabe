package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProteinShingleMapper extends Mapper <Object,Object, Text,Text>{

    private IntWritable shinglelength;
	
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
	
        Configuration conf = context.getConfiguration();
        shinglelength.set(conf.getInt("shingle.length",5));
    }
    
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
    	
        // take the Object value and store it into keyvals
        String keyvals = value.toString();
        
        // seperate key and value from each other
        String[] keyvals_split = keyvals.split("\t");
        Text newkey = new Text(keyvals_split[0]);
        String vals = keyvals_split[1];
        
        // Create Shingles by iterating through the value-String and taking Strings with the length of shinglelength
        for(int i = shinglelength.get(); i <= vals.length(); ++i){
        	
                Text outkey = new Text(vals.substring(i - shinglelength.get(), i));
                
                // write solution on context
                context.write(outkey,newkey);
        }
    }

}
