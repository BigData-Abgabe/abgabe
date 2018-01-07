package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MinhashMapper extends Mapper<Text, Text, Text, IntWritable> {

	Text protein = new Text();
	IntWritable shingle = new IntWritable();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
	}

	public void map(Text shingles, Text value, Context context) throws IOException, InterruptedException {

		protein.set(value.toString().substring(0, value.find("\t")));
		shingle.set(Integer.parseInt(shingles.toString()));

		context.write(protein, shingle);
	}
}
