package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;


public class ThreeLetterCodeReducer extends Reducer<Text, Text, Text, Text> {
	private Text sequence = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		for (Text value : values) {
			sequence.set(value); // all sequences should be the same so only one is needed
		}
	
		context.write(key, sequence);
	}
}