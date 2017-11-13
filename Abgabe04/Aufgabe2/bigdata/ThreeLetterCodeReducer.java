package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;


public class ThreeLetterCodeReducer extends Reducer<Text, Text, Text, Text> {
	private Text sequence = new Text();
	public void reduce(Text key, Text values, Context context)throws IOException, InterruptedException {
	
	
		context.write(key, values);
	}
}